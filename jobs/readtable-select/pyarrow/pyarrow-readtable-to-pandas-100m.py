import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    bench_lambda = lambda: ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas().size
    bench_name = 'pyarrow-readtable-to-pandas-100m'
    return (bench_name, bench_lambda, lambda : {})
