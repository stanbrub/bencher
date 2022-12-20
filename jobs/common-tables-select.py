# Assumes the variable 'tag' is defined in the environment.

import deephaven.parquet as pt

animals = pt.read('/data/animals.parquet').select()
adjectives = pt.read('/data/adjectives.parquet').select()
relation = pt.read('/data/relation-' + str(tag) + '.parquet').select()

