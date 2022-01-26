import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().select(["animal_id", "Values"]).to_pandas()
    df_list = []
    for sz in range(0, len(df.index), 5000000):
        df_list.append(df.head(sz))

    def after():
        nonlocal df
        del df
        nonlocal df_list
        del df_list
    def do():
        for df2 in df_list:
            g = df2.groupby(['animal_id']).sum()
        return len(g.index)
    bench_name = 'pyarrow-sumby-bench-no-nulls-100m-repeat'
    return (bench_name, do, after)
