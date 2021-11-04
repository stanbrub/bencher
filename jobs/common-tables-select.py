# Assumes the variable 'tag' is defined in the environment.

from deephaven.ParquetTools import readTable

animals = readTable('/data/animals.parquet').select()
adjectives = readTable('/data/adjectives.parquet').select()
relation = readTable('/data/relation-' + str(tag) + '.parquet').select()

