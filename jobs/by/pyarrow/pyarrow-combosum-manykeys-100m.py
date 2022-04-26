import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-manykeys-100m.parquet', format='parquet').to_table().select(['key1', 'Values']).to_pandas()
    def after():
        nonlocal df
        del df
    def do():
        g = df.groupby(['key1']).agg(Count=('Values', 'count'), Sum=('Values', 'sum'))
        return len(g.index)
    bench_name = 'pyarrow-combosum-manykeys'
    return (bench_name, do, after)
