Starburst OpenAPI
=============

This is a [Trino](http://trino.io/) plugin that provides a connector to read
from and write to HTTP APIs given an OpenAPI specification.

## Configuration

| Configuration property               | Description                                                                                              |
|--------------------------------------|----------------------------------------------------------------------------------------------------------|
| openapi.spec-location                | URL or filename containing the OpenAPI specification, either JSON or YAML                                |
| openapi.base-uri                     | Base URL for the API, often includes API version                                                         |
| openapi.max-requests-per-second      | Maximum HTTP requests per second executed from a single Trino node                                       |

The connector is using the Airlift HTTP client, which can be configured with
[additional
properties](https://github.com/airlift/airlift/blob/master/http-client/src/main/java/io/airlift/http/client/HttpClientConfig.java)
prefixed with `openapi`, like so:

```
openapi..http-client.log.enabled=true
openapi..http-client.log.path=logs
```
