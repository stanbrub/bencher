import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-manykeys-100m.parquet', format='parquet').to_table().select(['key1']).to_pandas()
    def after():
        nonlocal df
        del df
    def do():
        g = df.groupby(['key1']).count()
        return len(g.index)
    bench_name = 'pyarrow-countby-manykeys'
    return (bench_name, do, after)
