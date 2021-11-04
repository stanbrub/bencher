import pyarrow.dataset as ds
import time

df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()

start_time = time.time()
result = df['adjective_id'] * 643 + df['animal_id']
result = result.to_frame()
count = len(result)
end_time = time.time()

print(count)
print(end_time - start_time)
