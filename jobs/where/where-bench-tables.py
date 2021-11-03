# Assumes the variable 'tag' is defined in the environment.

from deephaven.ParquetTools import readTable

animals = readTable('/data/animals.parquet')
adjectives = readTable('/data/adjectives.parquet')
relation = ( readTable('/data/relation-' + str(tag) + '.parquet').join(adjectives, 'adjective_id')
             .join(animals, 'animal_id')
             .select('Values', 'adjective_name', 'animal_name') )
