# Assumes the variable 'tag' is defined in the environment.

from deephaven import parquet


animals = parquet.read('/data/animals.parquet')
adjectives = parquet.read('/data/adjectives.parquet')
relation = parquet.read('/data/relation-' + str(tag) + '.parquet')
