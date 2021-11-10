import pyarrow.dataset as ds
import pandas as pa
import time

workqueue = ds.dataset(output_prefix_path + '/data/workqueue-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()
auditqueue = ds.dataset(output_prefix_path + '/data/auditqueue-no-nulls-1m.parquet', format='parquet').to_table().to_pandas()

start_time = time.time()
count = pa.merge_asof(left=auditqueue, right=workqueue, on='timestamp', by='user_id')
end_time = time.time()

print(count)
print(end_time - start_time)
