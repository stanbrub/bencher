import pyarrow.dataset as ds
import time

df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-30m.parquet', format='parquet').to_table().to_pandas()[['animal_id', 'adjective_id']]
start_time = time.time(); count = len(df.groupby(['animal_id']).count().index); end_time = time.time()
print(count)
print(end_time - start_time)
