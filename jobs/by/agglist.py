from deephaven import agg

agg_list = [
    agg.count_("Count"),
    agg.avg("Avg = Values"),
    agg.std("Std = Values"),
]
