import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()[['animal_id', 'adjective_id']]
    def after():
        nonlocal df
        del df
    bench_lambda = lambda: len(df.groupby(['animal_id']).count().index)
    bench_name = 'pyarrow-select-by-bench-no-nulls-100m'
    return (bench_name, bench_lambda, after)
