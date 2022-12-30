print("Loading repeat function")
def doRepeat(relation, rps, fun):
    global processed_rows, time_start_ns, time_end_ns
    processed_rows = 0
    releaseSize = rps
    time_start_ns = time.perf_counter_ns()
    time_end_ns = time.perf_counter_ns()
    secDuration = 0
    while True:
        print("Release Size: {:,}, Duration: {:.2f}".format(releaseSize, secDuration))
        relHead = relation.head(releaseSize)
        fun(relHead)
        time_end_ns = time.perf_counter_ns()
        if (releaseSize >= relation.size):
            break
        nanoDuration = time_end_ns - time_start_ns
        secDuration = nanoDuration / 1000000000.0
        processed_rows += relHead.size
        releaseSize = int(rps * secDuration)