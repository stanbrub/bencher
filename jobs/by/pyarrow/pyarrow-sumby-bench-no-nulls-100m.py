import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    rel = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table()
    def after():
        nonlocal rel
        del rel
    def do():
        df = rel.to_pandas()
        g = df[['animal_id', 'Values']].groupby(['animal_id']).sum()
        return len(g.index)
    bench_name = 'pyarrow-by-bench-no-nulls-100m'
    return (bench_name, do, after)
