import pyarrow.dataset as ds
import time

df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()

start_time = time.time()
result = df..loc[:, ['adjective_id', 'animal_id']].apply(lambda row: row[0]*643 + row[1], axis=1, raw=True).to_frame()
count = len(df)
end_time = time.time()

print(count)
print(end_time - start_time)
