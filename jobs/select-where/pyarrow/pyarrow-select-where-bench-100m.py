import pyarrow.dataset as ds
import time

df = ds.dataset(output_prefix_path + '/data/relation-100m.parquet', format='parquet').to_table().to_pandas()
start_time = time.time(); count = len(df.query('adjective_id == animal_id').index); end_time = time.time()
print(count)
print(end_time - start_time)
