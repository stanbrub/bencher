import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    rel = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table()
    def after():
        nonlocal rel
        del rel
    def do():
        df = rel.to_pandas()
        df['composite_id'] = df['adjective_id'] * 643 + df['animal_id']
        return len(df)
    bench_name = 'pyarrow-update-bench-no-nulls-100m'
    return (bench_name, do, after)
