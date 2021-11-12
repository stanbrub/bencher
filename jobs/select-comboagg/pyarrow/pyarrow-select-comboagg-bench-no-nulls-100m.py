import pyarrow.dataset as ds
import pandas as pd

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()[['animal_id', 'adjective_id']]
    def after():
        nonlocal df
        del df
    def do():
        result = df.groupby(['animal_id'], sort=False).agg(
            SumAdj=pd.NamedAgg(column="adjective_id", aggfunc="sum"),
            MinAdj=pd.NamedAgg(column="adjective_id", aggfunc="min"),
            MaxAdj=pd.NamedAgg(column="adjective_id", aggfunc="max"),
            AvgAdj=pd.NamedAgg(column="adjective_id", aggfunc="mean"),
            StdAdj=pd.NamedAgg(column="adjective_id", aggfunc="std"),
            FirstAdj=pd.NamedAgg(column="adjective_id", aggfunc="first"),
            LastAdj=pd.NamedAgg(column="adjective_id", aggfunc="last"),
            CountAdj=pd.NamedAgg(column="adjective_id", aggfunc="count"),
        )
        return len(result.index)
    bench_name = 'pyarrow-select-comboagg-bench-no-nulls-100m'
    return (bench_name, do, after)
