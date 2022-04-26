from deephaven import Aggregation as agg, as_list

agg_list = as_list([\
    agg.AggCount("Count"),\
    agg.AggSum("Sum = Values"),\
    ])
