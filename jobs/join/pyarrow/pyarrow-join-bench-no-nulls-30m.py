import pyarrow.parquet as pq
import pandas as pd
import time

animals = pq.read_table(output_prefix_path + '/data/animals.parquet')
adjectives = pq.read_table(output_prefix_path + '/data/adjectives.parquet')
relation = pq.read_table(output_prefix_path + '/data/relation-no-nulls-30m.parquet')

start_time = time.time()
result = pd.merge(pd.merge(relation.to_pandas(), adjectives.to_pandas(), how='left', on='adjective_id'), animals.to_pandas(), how='left', on='animal_id')[['Values', 'adjective_name', 'animal_name']]
end_time = time.time()
print(len(result))
print(end_time - start_time)
