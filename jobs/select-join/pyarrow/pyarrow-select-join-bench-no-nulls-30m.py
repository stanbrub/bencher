import pyarrow.parquet as pq
import pandas as pd
import time

animals = pq.read_table(output_prefix_path + '/data/animals.parquet').to_pandas()
adjectives = pq.read_table(output_prefix_path + '/data/adjectives.parquet').to_pandas()
relation = pq.read_table(output_prefix_path + '/data/relation-no-nulls-30m.parquet').to_pandas()

start_time = time.time(); result = pd.merge(pd.merge(relation, adjectives, how='left', on='adjective_id'), animals, how='left', on='animal_id')[['Values', 'adjective_name', 'animal_name']]; end_time = time.time()
print(len(result))
print(end_time - start_time)
