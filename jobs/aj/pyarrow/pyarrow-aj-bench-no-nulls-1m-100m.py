import pyarrow.dataset as ds
import pandas as pd

def bench_definition(output_prefix_path):
    workqueue = ds.dataset(output_prefix_path + '/data/workqueue-no-nulls-100m.parquet', format='parquet').to_table()
    auditqueue = ds.dataset(output_prefix_path + '/data/auditqueue-no-nulls-1m.parquet', format='parquet').to_table()
    def after():
        nonlocal workqueue, auditqueue
        del workqueue
        del auditqueue
    def do():
        w = workqueue.to_pandas()
        a = auditqueue.to_pandas()
        df = pd.merge_asof(left=a, right=w, on='timestamp', by='user_id')
        return len(df.index)
    bench_name = 'pyarrow-aj-bench-no-nulls-1m-100m'
    return (bench_name, do, after)
