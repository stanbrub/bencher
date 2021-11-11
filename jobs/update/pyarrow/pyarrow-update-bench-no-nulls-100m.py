import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    d = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table()
    def after():
        nonlocal d, df
        del d
        del df
    def do():
        df = d.to_pandas()
        df['composite_id'] = df['adjective_id'] * 643 + df['animal_id']
        return len(df)
    bench_name = 'pyarrow-update-bench-no-nulls-100m'
    return (bench_name, do, after)
