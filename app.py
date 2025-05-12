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

def die_event(container_id, image_name, start_time, end_time, status):
    doc = {
        "containerId": container_id,
        "imageName": image_name,
        "startDate": datetime.fromtimestamp(start_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3],
        "endDate": datetime.fromtimestamp(end_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3] if end_time else "-",
        "duration": (end_time - start_time) if end_time else None,
        "startTimestamp": start_time,
        "endTimestamp": end_time,
        "status": status
    }
    try:
        es.index(index='container-availability', document=sanitize_keys(doc))
        es.index(index='active-containers', id=container_id ,document=sanitize_keys(doc))
        #print(sanitize_keys(doc))
    except Exception as e:
        print(f"Indexing error: {str(e)}")

def start_event(container_id, image_name, start_time, end_time, status):
    doc = {
        "containerId": container_id,
        "imageName": image_name,
        "startDate": datetime.fromtimestamp(start_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3],
        "endDate": datetime.fromtimestamp(end_time).strftime('%b %d, %Y @ %H:%M:%S.%f')[:-3] if end_time else "-",
        "duration": (end_time - start_time) if end_time else None,
        "startTimestamp": start_time,
        "endTimestamp": end_time,
        "status": status
    }

    try:
        es.index(index='active-containers', id=container_id ,document=sanitize_keys(doc))
        #print(sanitize_keys(doc))
    except Exception as e:
        print(f"Indexing error: {str(e)}")

for event in client.events(decode=True):
    if event['Type'] == 'container' and event['status'] in ('start', 'die'):
        actor = event['Actor']
        container_id = actor['ID']
        image_name = actor['Attributes'].get('image', 'unknown')
        timestamp = event['time']

        if event['status'] == 'start':
            start_cache[(container_id, image_name)].append(timestamp)
            start_event(container_id, image_name, timestamp, None , "active")

        elif event['status'] == 'die':
            cache_key = (container_id, image_name)
            if start_cache[cache_key]:
                start_time = start_cache[cache_key].popleft()
                die_event(container_id, image_name, start_time, timestamp, "disabled")
            else:
                # can handle orphaned die events
                pass