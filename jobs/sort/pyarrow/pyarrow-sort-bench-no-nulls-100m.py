import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    rel = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table()
    def after():
        nonlocal rel
        del rel
    def do():
        df = rel.to_pandas()
        s = df.sort_values(by=['animal_id', 'adjective_id'])
        return len(s.index)
    bench_name = 'pyarrow-sort-bench-no-nulls-100m'
    return (bench_name, do, after)
