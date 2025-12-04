Starburst OpenAPI
=============

This is a [Trino](http://trino.io/) plugin that provides a connector to read
from and write to HTTP APIs given an OpenAPI specification.

## Configuration

| Configuration property               | Description                                                                                              |
|--------------------------------------|----------------------------------------------------------------------------------------------------------|
| openapi.spec-location                | URL or filename containing the OpenAPI specification, either JSON or YAML                                |
| openapi.base-uri                     | Base URL for the API, often includes API version                                                         |
| openapi.authentication.type          | Default authentication type if not set in the specification. One of: `none`, `http`, `api_key`, `oauth`. |
| openapi.authentication.scheme        | Authentication scheme for the `http` authentication type. One of: `basic`, `bearer`.                     |
| openapi.authentication.client-id     | OAuth Client ID                                                                                          |
| openapi.authentication.client-secret | OAuth Client secret                                                                                      |
| openapi.authentication.username      | Username used for the `http` and `oauth` authentication types                                            |
| openapi.authentication.password      | Password used for the `http` and `oauth` authentication types                                            |
| openapi.authentication.bearer-token  | Bearer token for `http` authentication                                                                   |
| openapi.authentication.api-key-name  | API key name                                                                                             |
| openapi.authentication.api-key-value | API key value                                                                                            |
| openapi.authentication.api-keys      | A string of comma separated api key names followed by `=`'s followed by api key values.                  |
| openapi.max-requests-per-second      | Maximum HTTP requests per second executed from a single Trino node                                       |
| openapi.max-splits-per-second        | Maximum number of splits per second generated when executing a query                                     |
| openapi.domain-expansion-limit       | Maximum number of discrete domain values generated from range predicates, which correlates with the number of HTTP requests executed |

The connector is using the Airlift HTTP client, which can be configured with
[additional
properties](https://github.com/airlift/airlift/blob/master/http-client/src/main/java/io/airlift/http/client/HttpClientConfig.java)
prefixed with `openapi`, like so:

```
openapi..http-client.log.enabled=true
openapi..http-client.log.path=logs
```

## Known APIs

This connector has been tested with the following APIs. The Docker image
includes additional catalog files that will be used if the first of their
environmental variable is set.

| Name                                                                                              | Environmental variables      |
|---------------------------------------------------------------------------------------------------|------------------------------|
| [OpenAPI Petstore](https://github.com/OpenAPITools/openapi-petstore)                              | `PETSTORE_URL`               |
| [Starburst Galaxy](https://galaxy.starburst.io/public-api)                                        | `GALAXY_URL`, `GALAXY_TOKEN` |
| [Jira Cloud platform](https://developer.atlassian.com/cloud/jira/platform/rest/v3/intro/#version) | `JIRA_URL`                   |
| [GitHub REST API](https://docs.github.com/en/rest)                                                | `GITHUB_TOKEN`               |
| [DataDog API](https://docs.datadoghq.com/api/latest/)                                             | `DATADOG_URL`                |

## Schema mapping

The following rules are used to map the OpenAPI schema to SQL tables and columns:
* Paths are mapped to tables; table names are paths, with `camelCase` converted
  to `snake_case` and special characters (like `/`) converted to underscores.
* SQL operations are mapped to HTTP methods:
  * SELECT uses GET or POST, if no GET is available;
  * INSERT uses POST or PUT;
  * UPDATE uses PATCH or POST;
  * DELETE uses DELETE.
* All request parameters are mapped to columns, including path, query, and
  header parameters.
* Fields of the HTTP OK (200) response type are mapped to columns.
* Fields of the request body are also mapped to columns, if a POST request is
  used for SELECT.
* All columns are disambiguated - if a field with the same name but a different
  data type appears in the response and parameters, it'll be mapped to multiple
  columns, with numeric suffixes (`_2`, `_3`, etc.). Request body fields have a
  `_req` suffix.

## OpenAPI Extensions

OpenAPI allows using custom extensions - adding custom fields anywhere in the
schema as long as they're prefixed with an `X`. Such custom extensions are used
to fine-tune this connector.

If the original service cannot be modified to include an extension in a
generated OpenAPI schema, save it locally and modify as needed.

### Pagination

APIs can use 4 different types of pagination:
* Offset - every response can include an offset parameter, telling how many
  results to skip.
* Page - every response can include a page parameter; the number of results per
  page can be configurable with another parameter.
* Cursor/token - the response includes the value of a cursor or token,
  that needs to be included in the next request.
* Next page URL - the response includes an URL of the next set of results.

The connector currently supports only the `Page` pagination.

To enable pagination, add a `x-trino` section in the path's operation section:

```
paths:
  /records:
    get:
      responses:
        # ...
      x-trino:
       pageParam: "page"
       limitParam: "per-page"
       resultsPath: "$response.body#/workflows"
       totalResultsPath: "$response.body#/total_count"
```

### Error messages

When the connector makes an API call, for which the server responds with an
error, an SQL error is returned to the user. The error only contains the HTTP
status code, and the whole error response body is not visible to the user.
There are two reasons for this: if it's a JSON message, it might not be human
readable, and could also contain sensitive information.

To extract a human readable error message from error responses, set the
`errorPath` property in the `x-trino` section:

```
paths:
  /records:
    get:
      responses:
        # ...
      x-trino:
       errorPath: "$response.body#/message"
```
