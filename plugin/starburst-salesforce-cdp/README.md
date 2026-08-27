## Salesforce Customer Data Platform connector

## About the Salesforce CDP JDBC Driver

### Overview

The full documentation for the Salesforce CDP JDBC can be found at https://github.com/forcedotcom/Salesforce-CDP-jdbc.

### Deploying JDBC Driver to Starburst Maven

Described at https://starburstdata.atlassian.net/l/cp/dk0YG8U8

### Custom PreparedStatement

Salesforce CDP jdbc driver does not support PreparedStatements and as base-jdbc required this functionality.
To work around this limitation custom `SalesforceCdpEmulatedPreparedStatement` was implemented to convert query with parameters to plain query.

## Custom Tables and Columns in Salesforce

Salesforce identifies any custom objects a user creates by adding `__c` to the column name and `__dll` to table name.
In order to use `BaseJdbcConnectorTest` for `TestSalesforceCdpConnectorTest` `SalesforceCdpIdentifierMapping` is created to map names accordingly.

## Data Type Mappings

Supported types in Salesforce are referenced here: https://help.salesforce.com/s/articleView?id=sf.c360_a_data_types.htm&type=5
which are mapped to Jdbc and SPI types as:

| Salesforce type | JDBC type | SPI type            |
|-----------------|-----------|---------------------|
| Text            | VARCHAR   | VARCHAR             |
| Number          | NUMERIC   | DOUBLE              |
| Datetime        | TIMESTAMP | TIMESTAMP_TZ_MILLIS |
| Date            | DATE      | DATE                |
| Email           | VARCHAR   | VARCHAR             |
| Phone           | VARCHAR   | VARCHAR             |
| Url             | VARCHAR   | VARCHAR             |
| Percent         | NUMERIC   | DOUBLE              |
| Boolean         | BOOLEAN   | BOOLEAN             |

## Testing account

Described at https://starburstdata.atlassian.net/wiki/spaces/STARBURST/pages/3401908331/Salesforce+Customer+Data+Platform+CDP+setup

## CI failure
Salesforce account used in CI has rate-limiting set which causes tests to fail with following errors.

```
Invalid batch Id: 720885d3-931c-45c1-a97b-ba31e46ae5cf
```
```
Token exchange failed. Please login again.
```
```
Failed to get config from the server
```
