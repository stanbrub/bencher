import pyarrow.dataset as ds

def bench_definition(output_prefix_path):
    df = ds.dataset(output_prefix_path + '/data/relation-no-nulls-100m.parquet', format='parquet').to_table().to_pandas().head(10000000)
    df_list = []
    for sz in range(0, len(df.index), 500000):
        df_list.append(df.head(sz))

    def after():
        nonlocal df
        del df
        nonlocal df_list
        del df_list
    bench_lambda = lambda: sum(map(lambda df2 : len(df2.sort_values(by=['animal_id']).index), df_list))
    bench_name = 'pyarrow-select-sort-bench-1col-no-nulls-10m-repeat'
    return (bench_name, bench_lambda, after)
