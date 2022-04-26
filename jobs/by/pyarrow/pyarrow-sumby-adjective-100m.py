import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().select(["adjective_id", "Values"]).to_pandas()
    def after():
        #nonlocal rel
        #del rel
        nonlocal df
        del df
    def do():
        g = df.groupby(['adjective_id']).sum()
        return len(g.index)
    bench_name = 'pyarrow-sumby-adjective'
    return (bench_name, do, after)
