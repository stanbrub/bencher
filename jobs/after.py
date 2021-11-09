import os.path
import time
import datetime
import pytz
import deephaven.TableLoggers as tl
import deephaven.TableTools as tt
import csv

#
# Capture how much time was spent in gc during the benchmark operation; if this number
# is comparable to the ellapsed benchmark time, then our results are invalidated and
# we need to run with more memory / bigger Java heap.
#
runtime_memory.read(runtime_memory_pos)
gc_seconds = \
    (runtime_memory_pos.totalCollectionTimeMs - runtime_memory_pre.totalCollectionTimeMs) / 1000.0

#
# The variables 'time_start_ns', 'time_end_ns' and 'bench_name' should
# have been defined previously in the execution environment.
#

ellapsed_seconds = (time_end_ns - time_start_ns) / (1000*1000*1000.0)

# Ensure process information is kept so we know the characteristics of the machine where the test ran.
# The following code will ensure a csv file with process information is saved, if it does not already exist.
# The filename matches the unique id of the DH worker process, eg: '41d68cea-1407-4811-9ce0-017bc259fd8b.csv'
# The row we will insert with the result later will reference the process unique id where the results where
# generated.

process_unique_id = ( jpy.get_type("io.deephaven.db.v2.utils.MemoryTableLoggers")
                      .getInstance().getProcessInfo().getId().value() )
process_info_path = '/data/' + process_unique_id + ".csv"

if not os.path.exists(process_info_path):
    tt.writeCsv(tl.processInfoLog(), process_info_path, ["Type", "Key", "Value"])

now = time.time()
timestamp_utc = datetime.datetime.fromtimestamp(now, pytz.UTC)
timestamp_nyc = datetime.datetime.fromtimestamp(now, pytz.timezone('America/New_York'))

# Append to bench-results.csv our results.
header = [ 'bench_name', 'timestamp_nyc', 'timestamp_utc', 'process_unique_id', 'gc_seconds', 'ellapsed_seconds' ]
fields = [ bench_name, timestamp_nyc, timestamp_utc, process_unique_id, gc_seconds, ellapsed_seconds ]
results_file = '/data/bench-results.csv'
need_header = not os.path.exists(results_file)
with open(results_file, 'a') as file:
    writer = csv.writer(file)
    if need_header:
        writer.writerow(header)
    writer.writerow(fields)

try:
    bench_cleanup()
except Exception:
    pass
