# Assumes the variable 'tag' is defined in the environment.

from deephaven import parquet


animals = parquet.read('/data/animals.parquet')
adjectives = parquet.read('/data/adjectives.parquet')
words10k = parquet.read('/data/10kwords.parquet')
words100k = parquet.read('/data/100kwords.parquet')
hex1mm = parquet.read('/data/hex1mm.parquet')
relation_join = parquet.read('/data/relation-join.parquet')
