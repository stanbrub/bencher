package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class RandomGenerator extends DataGenerator {

    private final PercentNullManager percent_null;
    private final GeneratorObjectIterator objectIterator;

    private final PrimitiveIterator<?, ?> it;

    private RandomGenerator(
            final ColumnType columnType, final long seed, final double percent_null, final PrimitiveIterator<?, ?> it) {
        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        objectIterator = new GeneratorObjectIterator();
        this.columnType = columnType;
        this.it = it;
    }

    static RandomGenerator ofUniformInt(
            final ColumnType columnType,
            final int lower_bound,
            final int upper_bound,
            final long seed,
            final double percent_null
    ) {
        final IntStream is = new Random(seed).ints(lower_bound, upper_bound);
        return new RandomGenerator(columnType, seed, percent_null, is.iterator());
    }

    static RandomGenerator ofUniformDouble(
            final ColumnType columnType,
            final double lower_bound,
            final double upper_bound,
            final long seed,
            final double percent_null
    ) {
        final DoubleStream ds = new Random(seed).doubles(lower_bound, upper_bound);
        return new RandomGenerator(columnType, seed, percent_null, ds.iterator());
    }

    static RandomGenerator ofUniformLong(
            final ColumnType columnType,
            final long lower_bound,
            final long upper_bound,
            final long seed,
            final double percent_null
    ) {
        final LongStream ls = new Random(seed).longs(lower_bound, upper_bound);
        return new RandomGenerator(columnType, seed, percent_null, ls.iterator());
    }

    @Override
    public Iterator<Object> getIterator() {
        return objectIterator;
    }

    static RandomGenerator fromJson(final String fieldName, final JSONObject jo) {

        final ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        final double percent_null = PercentNullManager.parseJson(fieldName, jo);

        final Object jsonLower = jo.get("lower_bound");
        if (jsonLower == null) {
            throw new IllegalArgumentException("Missing \"lower_bound\" element");
        } else if (! (jsonLower instanceof String)) {
            throw new IllegalArgumentException("Wrong type for \"lower_bound\" element, should be string");
        }
        final String lower = (String) jsonLower;

        final Object jsonUpper = jo.get("upper_bound");
        if (jsonUpper == null) {
            throw new IllegalArgumentException("Missing \"upper_bound\" element");
        } else if (! (jsonUpper instanceof String)) {
            throw new IllegalArgumentException("Wrong type for \"upper_bound\" element, should be string");
        }
        final String upper = (String) jsonUpper;

        final Object jsonSeed = jo.get("seed");
        if (jsonSeed == null) {
            throw new IllegalArgumentException("Missing \"upper_bound\" element");
        } else if (! (jsonSeed instanceof String)) {
            throw new IllegalArgumentException("Wrong type for \"seed\" element, should be string");
        }
        final long seed;
        try {
            seed = Long.parseLong((String) jsonSeed);
        } catch( NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Can't convert element value \"%s\" for \"seed\" element", jsonSeed),
                    ex);
        }

        switch (columnType) {
            case DOUBLE:
            {
                final double lower_bound = Double.parseDouble(lower);
                final double upper_bound = Double.parseDouble(upper);

                return RandomGenerator.ofUniformDouble(columnType, lower_bound, upper_bound, seed, percent_null);
            }

            case INT32:
            {
                final int lower_bound = Integer.parseInt(lower);
                final int upper_bound = Integer.parseInt(upper);

                return RandomGenerator.ofUniformInt(columnType, lower_bound, upper_bound, seed, percent_null);
            }

            case INT64:
            {
                final long lower_bound = Long.parseLong(lower);
                final long upper_bound = Long.parseLong(upper);

                return RandomGenerator.ofUniformLong(columnType, lower_bound, upper_bound, seed, percent_null);
            }

            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException(String.format("%s: output type %s is not supported", fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    private Object getNext() {
        switch (columnType) {
            case DOUBLE:
            case INT32:
            case INT64:
                return it.next();

            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalStateException(String.format("column type %s is not supported", columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    private class GeneratorObjectIterator implements Iterator<Object> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Object next() {

            // consume from the iterator ...
            Object o = getNext();

            // even if we end up rolling a null
            if (percent_null != null && percent_null.test()) {
                return null;
            }

            return o;
        }
    }
}

