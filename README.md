
# Overview
Code to query EVM events from Solidity Contracs and use for analytics.
# Setup
Building the container takes quite a while due to the rustup installation and cryo setup.
``````
docker-compose up -d
docker-compose exec sandbox bash
cd /code
```
This runs the container and mounts all files of the repo to /code - from here we can live develop