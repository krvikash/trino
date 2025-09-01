Data generated using PyArrow:

```py
import pyarrow as pa
import pyarrow.parquet as pq

schema = pa.schema([("_time", pa.time32("ms"))])
table = pa.Table.from_arrays([pa.array([0, 45296123, 86399999], type=pa.time32("ms"))], schema=schema)
pq.write_table(table, "time_millis.parquet")
```
