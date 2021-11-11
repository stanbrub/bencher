import csv
import datetime
import pytz
import os
import sys
import time

def run_bench(bench_name : str, bench_fun, after_fun, output_prefix_path : str):
    start_time = time.time()
    count = bench_fun()
    end_time = time.time()
    after_fun()
    print(count)
    elapsed_seconds = end_time - start_time
    print(elapsed_seconds)
    timestamp_utc = datetime.datetime.fromtimestamp(end_time, pytz.UTC)
    timestamp_nyc = datetime.datetime.fromtimestamp(end_time, pytz.timezone('America/New_York'))
    header = [ 'bench_name', 'timestamp_nyc', 'timestamp_utc', 'elapsed_seconds' ]
    fields = [ bench_name, timestamp_nyc, timestamp_utc, elapsed_seconds ]
    results_file = output_prefix_path + '/data/pyarrow-bench-results.csv'
    need_header = not os.path.exists(results_file)
    with open(results_file, 'a') as file:
        writer = csv.writer(file)
        if need_header:
            writer.writerow(header)
        writer.writerow(fields)
    return elapsed_seconds

if len(sys.argv) < 3:
    print(f"Usage: {sys.argv[0]} output_prefix_path pyarrow_benchmark.py [pyarrow-benchmark2.py ...]", file=sys.stderr)
    sys.exit(1)

output_prefix_path = sys.argv[1]
benchmarks = sys.argv[2:]

for benchmark in benchmarks:
    exec(open(benchmark).read())
    (bench_name, bench_fun, after_fun) = bench_definition(output_prefix_path)
    print(f"Running {benchmark}...")
    elapsed_seconds = run_bench(bench_name, bench_fun, after_fun, output_prefix_path)
    print(f"Ran {benchmark} in {elapsed_seconds} seconds.")
