# stargate-buffer-service

## Docker images build

For images build `./bin/build-docker-image.sh` script is used. 

```shell
❯ ./bin/build-docker-image.sh -h
Usage:
    ./bin/build-docker-image.sh [option]
Options:
        -h    Help
        -p    Project to build (data-server|discovery-server)
        -r    Docker repository
        -v    Project version
        -a    Platform types for multi-arch build
```
example
```shell
./bin/build-docker-image.sh -p data-server -r example.repo.com/some_repo -v data-server-999-vg4ws65
```
For local development it may be used as follows
```shell
./bin/build-docker-image.sh -p data-server
./bin/build-docker-image.sh -p discovery-server
```
