#!/bin/bash


# Install curl inside the container
docker exec -it ptl-presto-master apt install -y curl

# Download trino-cli and move it to /tmp directory
docker exec -it ptl-presto-master curl https://s3.us-east-2.amazonaws.com/software.starburstdata.net/391e/391-e/trino-cli-391-executable.jar --output /tmp/a.jar

# Move and rename the downloaded file
docker exec -it ptl-presto-master mv /tmp/a.jar /tmp/trino

# Make the file executable
docker exec -it ptl-presto-master chmod +x /tmp/trino

docker exec -it ptl-presto-master /tmp/trino
