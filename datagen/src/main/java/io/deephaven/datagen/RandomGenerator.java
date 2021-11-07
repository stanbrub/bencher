package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class RandomGenerator extends DataGenerator {

    private final PercentNullManager pctNullMgr;
    private final GeneratorObjectIterator objectIterator;

    private final PrimitiveIterator<?, ?> it;

    private RandomGenerator(
            final ColumnType columnType, final long seed, final double pctNullMgr, final PrimitiveIterator<?, ?> it) {
        this.pctNullMgr = PercentNullManager.fromPercentage(pctNullMgr, seed);
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

    static RandomGenerator ofNormal(
            final double mean,
            final double stddev,
            final long seed,
            final double percent_null
    ) {
        return new RandomGenerator(ColumnType.DOUBLE, seed, percent_null, new PrimitiveIterator.OfDouble() {
            final Random prng = new Random(seed);
            boolean havePrecomputed = false;
            double precomputed;
            @Override
            public double nextDouble() {
                // Uses polar Box-Muller transformation, which generates two values at a time.
                if (havePrecomputed) {
                    havePrecomputed = false;
                    return mean + stddev*precomputed;
                };

                double x, y, sq;
                do {
                    x = 2.0 * prng.nextDouble() - 1.0;
                    y = 2.0 * prng.nextDouble() - 1.0;
                    sq = x*x + y*y;
                } while (sq >= 1.0);

                final double z = Math.sqrt(-2.0*Math.log(sq)/sq);
                havePrecomputed = true;
                precomputed = x*z;
                return mean + stddev*y*z;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        });
    }

    private static double nextExp(final Random prng, final double inverseLambda) {
        double x;
        do {
            x = prng.nextDouble();
        } while (x == 0.0);
        return -Math.log(x) * inverseLambda;
    }

    static RandomGenerator ofPoissonWait(
            final long startNanos,
            final long periodNanos,
            final long seed,
            final double percent_null
    ) {
        if (periodNanos <= 0) {
            throw new IllegalArgumentException(String.format("period_nanos (=%d) should be > 0.", periodNanos));
        }
        return new RandomGenerator(ColumnType.TIMESTAMP_NANOS, seed, percent_null, new PrimitiveIterator.OfLong() {
            final Random prng = new Random(seed);
            final double period = (double) periodNanos;
            long current = startNanos;
            @Override
            public long nextLong() {
                current += (long) Math.floor(nextExp(prng, period));
                return current;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        });
    }

    static RandomGenerator ofExponential(
            final double lambda,
            final long seed,
            final double percent_null
    ) {
        if (lambda <= 0) {
            throw new IllegalArgumentException(String.format("lambda (=%g) should be > 0", lambda));
        }
        return new RandomGenerator(ColumnType.DOUBLE, seed, percent_null, new PrimitiveIterator.OfDouble() {
            final Random prng = new Random(seed);
            final double inverseLambda = 1.0 / lambda;
            @Override
            public double nextDouble() {
                return nextExp(prng, inverseLambda);
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        });
    }

    private static int minusOneOrOne(final Random prng) {
        final int zeroOrOne = prng.nextInt(2);
        final int minusOneOrOne = 2 * zeroOrOne - 1;
        return minusOneOrOne;
    }

    static RandomGenerator ofRandomWalkInt(
            final int initial,
            final int step,
            final long seed,
            final double percent_null
    ) {
        return new RandomGenerator(ColumnType.INT32, seed, percent_null, new PrimitiveIterator.OfInt() {
            final Random prng = new Random(seed);
            int current = initial;
            @Override
            public int nextInt() {
                current += minusOneOrOne(prng) * step;
                return current;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        });
    }

    static RandomGenerator ofRandomWalkLong(
            final long initial,
            final long step,
            final long seed,
            final double percent_null
    ) {
        return new RandomGenerator(ColumnType.INT64, seed, percent_null, new PrimitiveIterator.OfLong() {
            final Random prng = new Random(seed);
            long current = initial;
            @Override
            public long nextLong() {
                current += minusOneOrOne(prng) * step;
                return current;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        });
    }

    static RandomGenerator ofRandomWalkDouble(
            final double initial,
            final double step,
            final long seed,
            final double percent_null
    ) {
        return new RandomGenerator(ColumnType.DOUBLE, seed, percent_null, new PrimitiveIterator.OfDouble() {
            final Random prng = new Random(seed);
            double current = initial;
            @Override
            public double nextDouble() {
                current += minusOneOrOne(prng) * step;
                return current;
            }

            @Override
            public boolean hasNext() {
                return true;
            }
        });
    }

    @Override
    public Iterator<Object> getIterator() {
        return objectIterator;
    }

    static RandomGenerator uniformFromJson(
            final String fieldName,
            final JSONObject jo,
            final ColumnType columnType,
            final double percentNull,
            final long seed) {

        switch (columnType) {
            case INT32: {
                final int lower_bound = Utils.getIntElementValue("lower_bound", jo);
                final int upper_bound = Utils.getIntElementValue("upper_bound", jo);

                return RandomGenerator.ofUniformInt(columnType, lower_bound, upper_bound, seed, percentNull);
            }

            case INT64: {
                final long lower_bound = Utils.getLongElementValue("lower_bound", jo);
                final long upper_bound = Utils.getLongElementValue("upper_bound", jo);

                return RandomGenerator.ofUniformLong(columnType, lower_bound, upper_bound, seed, percentNull);
            }

            case DOUBLE: {
                final double lower_bound = Utils.getDoubleElementValue("lower_bound", jo);
                final double upper_bound = Utils.getDoubleElementValue("upper_bound", jo);

                return RandomGenerator.ofUniformDouble(columnType, lower_bound, upper_bound, seed, percentNull);
            }

            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException(String.format("%s: output type %s is not supported", fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    static RandomGenerator normalFromJson(
            final String fieldName,
            final JSONObject jo,
            final ColumnType columnType,
            final double percentNull,
            final long seed) {
        final double mean = Utils.getDoubleElementValue("mean", jo);
        final double stddev = Utils.getDoubleElementValue("stddev", jo);
        switch (columnType) {
            case DOUBLE:
                return RandomGenerator.ofNormal(mean, stddev, seed, percentNull);
            case INT32:
            case INT64:
            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException(String.format(
                        "%s: output type %s is not supported for normal distribution",
                        fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    static RandomGenerator exponentialFromJson(
            final String fieldName,
            final JSONObject jo,
            final ColumnType columnType,
            final double percentNull,
            final long seed) {
        final double lambda = Utils.getDoubleElementValue("lambda", jo);
        switch (columnType) {
            case DOUBLE:
                return RandomGenerator.ofExponential(lambda, seed, percentNull);
            case INT32:
            case INT64:
            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException(String.format(
                        "%s: output type %s is not supported for normal distribution",
                        fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    static RandomGenerator poissonWaitFromJson(
            final String fieldName,
            final JSONObject jo,
            final ColumnType columnType,
            final double percentNull,
            final long seed) {
        final long startNanos = Utils.getLongElementValue("start_nanos", jo);
        final long periodNanos = Utils.getLongElementValue("period_nanos", jo);
        switch (columnType) {
            case TIMESTAMP_NANOS:
                return RandomGenerator.ofPoissonWait(startNanos, periodNanos, seed, percentNull);
            case INT32:
            case INT64:
            case DOUBLE:
            case STRING:
                throw new IllegalArgumentException(String.format(
                        "%s: output type %s is not supported for poisson-wait distribution",
                        fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    static RandomGenerator randomWalkFromJson(
            final String fieldName,
            final JSONObject jo,
            final ColumnType columnType,
            final double percentNull,
            final long seed) {
        switch (columnType) {
            case INT32: {
                final int initial = Utils.getIntElementValue("initial", jo);
                final int step = Utils.getIntElementValue("step", jo);
                return RandomGenerator.ofRandomWalkInt(initial, step, seed, percentNull);
            }
            case INT64: {
                final long initial = Utils.getLongElementValue("initial", jo);
                final long step = Utils.getLongElementValue("step", jo);
                return RandomGenerator.ofRandomWalkLong(initial, step, seed, percentNull);
            }
            case DOUBLE: {
                final double initial = Utils.getDoubleElementValue("initial", jo);
                final double step = Utils.getDoubleElementValue("step", jo);
                return RandomGenerator.ofRandomWalkDouble(initial, step, seed, percentNull);
            }
            case TIMESTAMP_NANOS:
            case STRING:
                throw new IllegalArgumentException(String.format(
                        "%s: output type %s is not supported for poisson-wait distribution",
                        fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }


    static RandomGenerator fromJson(final String fieldName, final JSONObject jo) {

        final ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        final double percentNull = PercentNullManager.parseJson(fieldName, jo);

        final long seed = Utils.getLongElementValue("seed", jo);
        final String distribution = Utils.getStringElementValue("distribution", jo);

        switch (distribution) {
            case "exponential":
                return exponentialFromJson(fieldName, jo, columnType, percentNull, seed);
            case "uniform":
                return uniformFromJson(fieldName, jo, columnType, percentNull, seed);
            case "normal":
                return normalFromJson(fieldName, jo, columnType, percentNull, seed);
            case "poisson_wait":
                return poissonWaitFromJson(fieldName, jo, columnType, percentNull, seed);
            case "random_walk":
                return randomWalkFromJson(fieldName, jo, columnType, percentNull, seed);
            default:
                throw new IllegalArgumentException(String.format(
                        "Unrecognized value \"%s\" for \"distribution\" element", distribution));
        }
    }

    private Object getNext() {
        return it.next();
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
            if (pctNullMgr.test()) {
                return null;
            }

            return o;
        }
    }
}

