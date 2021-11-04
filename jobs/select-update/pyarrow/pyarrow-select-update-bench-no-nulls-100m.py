import pyarrow.dataset as ds
import time

df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()
u = df.apply(lambda an, ad: ad*643 + an, axis=1, raw=True, result_type='broadcast')
start_time = time.time(); count = len(df.loc[query('adjective_id == animal_id').index); end_time = time.time()
print(count)
print(end_time - start_time)
