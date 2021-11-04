import pyarrow.dataset as ds
import time

d = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table()

start_time = time.time()
df = d.to_pandas()
df['composite_id'] = df..loc[:, ['adjective_id', 'animal_id']].apply(lambda row: row[0]*643 + row[1], axis=1, raw=True)
count = len(df)
end_time = time.time()

print(count)
print(end_time - start_time)
