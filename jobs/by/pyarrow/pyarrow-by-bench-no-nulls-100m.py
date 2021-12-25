import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    rel = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().select(['animal_id', 'adjective_id'])
    def after():
        nonlocal rel
        del rel
    def do():
        df = rel.to_pandas()
        g = df.groupby(['animal_id']).count()
        return len(g.index)
    bench_name = 'pyarrow-by-bench-no-nulls-100m'
    return (bench_name, do, after)
