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
    def do():
        re = relation.to_pandas()
        ad = adjectives.to_pandas()
        an = animals.to_pandas()
        j1 = pd.merge(re, ad, how='left', on='adjective_id')
        j2 = pd.merge(j1, an, how='left', on='animal_id')[['Values', 'adjective_name', 'animal_name']]
        return len(j2.index)
    bench_name = 'pyarrow-join-bench-no-nulls-100m'
    return (bench_name, do, after)
