import pyarrow.dataset as ds
import time

start_time = time.time(); count = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table(filter=(ds.field('adjective_id') == ds.field('animal_id'))).num_rows; end_time = time.time()
print(count)
print(end_time - start_time)
