# docker-container-availability-exporter

Service for listening on given docker events and pushing them directly to the ELK stack

# Usage

This service is designed to be run as a docker container.

# Design

This is a simple service that functions as a pre-processor for the docker events in order to specifically create
availability report even for the same containers that can be started and stopped multiple times.

It uses a simple cache mechanism to store the events and when the container is stopped, it will push the event to the
ELK stack.

The service is using the Python `elasticsearch` library to send events to the ELK stack. The service is designed to be
run as a docker container. The service is using the `docker` library to listen on docker events. The service is using
the
`docker` library to listen on docker events. More regarding the library
here https://elasticsearch-py.readthedocs.io/en/v8.18.1/


