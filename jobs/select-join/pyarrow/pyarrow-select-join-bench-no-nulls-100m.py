import pyarrow.parquet as pq
import pandas as pd

def bench_definition(output_prefix_path):
    animals = pq.read_table(output_prefix_path + '/data/animals.parquet').to_pandas()
    adjectives = pq.read_table(output_prefix_path + '/data/adjectives.parquet').to_pandas()
    relation = pq.read_table(output_prefix_path + '/data/relation-no-nulls-100m.parquet').to_pandas()
    def after():
        nonlocal animals, adjectives, relation
        del animals
        del adjectives
        del relation
    bench_lambda = lambda: len(pd.merge(pd.merge(relation, adjectives, how='left', on='adjective_id'), animals, how='left', on='animal_id')[['Values', 'adjective_name', 'animal_name']].index)
    bench_name = 'pyarrow-select-join-bench-no-nulls-100m'
    return (bench_name, bench_lambda, after)
