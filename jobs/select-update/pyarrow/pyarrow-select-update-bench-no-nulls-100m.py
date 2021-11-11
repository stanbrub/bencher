import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas()
    def after():
        nonlocal df
        del df
    def do():
        df['composite_id'] = df['adjective_id'] * 643 + df['animal_id']
        return len(df.index)
    bench_name = 'pyarrow-select-update-bench-no-nulls-100m'
    return (bench_name, do, after)
