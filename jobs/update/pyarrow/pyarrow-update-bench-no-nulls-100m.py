import pyarrow.dataset as ds
import time

d = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table()

start_time = time.time()
df = d.to_pandas()
df['composite_id'] = df['adjective_id'] * 643 + df['animal_id']
count = len(df)
end_time = time.time()

print(count)
print(end_time - start_time)
