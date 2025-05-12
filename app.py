import docker
from elasticsearch import Elasticsearch
from collections import defaultdict, deque
from datetime import datetime
import threading
import time

es = Elasticsearch(hosts=['http://elk:9200'])
client = docker.from_env()

# Cache to track active start events: {(container_id, image_name): deque(start_times)}
start_cache = defaultdict(deque)

def sanitize_keys(obj):
    if isinstance(obj, dict):
        return {k.replace('.', '_'): sanitize_keys(v) for k, v in obj.items()}
    return obj

def send_combined_event(container_id, image_name, start_time, end_time):
    doc = {
        "containerId": container_id,
        "imageName": image_name,
        "startDate": datetime.fromtimestamp(start_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3],
        "endDate": datetime.fromtimestamp(end_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3] if end_time else "-",
        "duration": (end_time - start_time) if end_time else None,
        "startTimestamp": start_time,
        "endTimestamp": end_time
    }
    try:
        es.index(index='container-availability', document=sanitize_keys(doc))
        #print(sanitize_keys(doc))
    except Exception as e:
        print(f"Indexing error: {str(e)}")

def send_active_containers():
    """
    Periodically sends information about active containers (started but not ended)
    to a separate Elasticsearch index.
    """
    while True:
        active_containers = []
        current_time = time.time()

        # Iterate through the start_cache to find active containers
        for (container_id, image_name), start_times in start_cache.items():
            # If there are start times in the deque, the container is active
            for start_time in start_times:
                doc = {
                    "containerId": container_id,
                    "imageName": image_name,
                    "startDate": datetime.fromtimestamp(start_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3],
                    "startTimestamp": start_time,
                    "activeFor": current_time - start_time  # Duration since start in seconds
                }
                active_containers.append(doc)

        # Send active containers to a separate index
        for doc in active_containers:
            try:
                es.index(index='active-containers', document=sanitize_keys(doc))
                #print(sanitize_keys(doc))
            except Exception as e:
                print(f"Error indexing active container: {str(e)}")

        # Wait for 30 seconds before the next check
        time.sleep(30)

# Start the monitoring thread
monitoring_thread = threading.Thread(target=send_active_containers, daemon=True)
monitoring_thread.start()


for event in client.events(decode=True):
    if event['Type'] == 'container' and event['status'] in ('start', 'die'):
        actor = event['Actor']
        container_id = actor['ID']
        image_name = actor['Attributes'].get('image', 'unknown')
        timestamp = event['time']

        if event['status'] == 'start':
            start_cache[(container_id, image_name)].append(timestamp)

        elif event['status'] == 'die':
            cache_key = (container_id, image_name)
            if start_cache[cache_key]:
                start_time = start_cache[cache_key].popleft()
                send_combined_event(container_id, image_name, start_time, timestamp)
            else:
                # can handle orphaned die events
                pass