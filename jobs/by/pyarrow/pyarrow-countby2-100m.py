import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().select(['animal_id', 'adjective_id']).to_pandas()
    def after():
        nonlocal df
        del df
    def do():
        g = df.groupby(['animal_id', 'adjective_id']).count()
        return len(g.index)
    bench_name = 'pyarrow-countby2-100m'
    return (bench_name, do, after)
