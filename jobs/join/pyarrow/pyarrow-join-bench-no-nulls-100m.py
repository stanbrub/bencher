import pyarrow.parquet as pq
import pandas as pd

def bench_definition(output_prefix_path):
    animals = pq.read_table(output_prefix_path + '/data/animals.parquet')
    adjectives = pq.read_table(output_prefix_path + '/data/adjectives.parquet')
    relation = pq.read_table(output_prefix_path + '/data/relation-no-nulls-30m.parquet')
    def after():
        nonlocal animals, adjectives, relation
        del animals
        del adjectives
        del relation
    bench_lambda = lambda: len(pd.merge(pd.merge(relation.to_pandas(), adjectives.to_pandas(), how='left', on='adjective_id'), animals.to_pandas(), how='left', on='animal_id')[['Values', 'adjective_name', 'animal_name']].index)
    bench_name = 'pyarrow-join-bench-no-nulls-100m'
    return (bench_name, bench_lambda, after)
