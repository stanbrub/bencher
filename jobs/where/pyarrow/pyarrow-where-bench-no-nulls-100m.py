import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    bench_lambda = lambda: ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table(filter=(ds.field('adjective_id') == ds.field('animal_id'))).num_rows
    bench_name = 'pyarrow-where-bench-no-nulls-100m'
    return (bench_name, bench_lambda, lambda: None)
