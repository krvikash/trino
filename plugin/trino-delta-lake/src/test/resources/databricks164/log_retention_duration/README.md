Data generated using Databricks 16.4:

```sql
CREATE TABLE log_retention_duration (
id INT,
val INT
) USING DELTA
LOCATION ?
TBLPROPERTIES (
'delta.logRetentionDuration' = '10 SECONDS',
'delta.checkpointInterval' = '2'
);

INSERT INTO log_retention_duration values
(1, 1),
(2, 2);
```
