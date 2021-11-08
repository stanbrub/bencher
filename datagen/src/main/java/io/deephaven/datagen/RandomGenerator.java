package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.*;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class RandomGenerator extends DataGenerator {

    private final PercentNullManager pctNullMgr;
    private final GeneratorObjectIterator objectIterator;

    private final Iterator<?> it;

    private RandomGenerator(
            final ColumnType columnType, final long seed, final double pctNullMgr, final Iterator<?> it) {
        this.pctNullMgr = PercentNullManager.fromPercentage(pctNullMgr, seed);
        objectIterator = new GeneratorObjectIterator();
        this.columnType = columnType;
        this.it = it;
    }

    static RandomGenerator ofUniformInt(
            final ColumnType columnType,
            final int lowerBound,
            final int upperBound,
            final long seed,
            final double percentNull
    ) {
        final IntStream is = new Random(seed).ints(lowerBound, upperBound);
        return new RandomGenerator(columnType, seed, percentNull, is.iterator());
    }

    static RandomGenerator ofUniformDouble(
            final ColumnType columnType,
            final double lowerBound,
            final double upperBound,
            final long seed,
            final double percent_null
    ) {
        final DoubleStream ds = new Random(seed).doubles(lowerBound, upperBound);
        return new RandomGenerator(columnType, seed, percent_null, ds.iterator());
    }

    static RandomGenerator ofUniformLong(
            final ColumnType columnType,
            final long lowerBound,
            final long upperBound,
            final long seed,
            final double percent_null
    ) {
        final LongStream ls = new Random(seed).longs(lowerBound, upperBound);
        return new RandomGenerator(columnType, seed, percent_null, ls.iterator());
    }

    static RandomGenerator ofNormal(
            final double mean,
            final double stddev,
            final long seed,
            final double percentNull
    ) {
        return new RandomGenerator(ColumnType.DOUBLE, seed, percentNull, new PrimitiveIterator.OfDouble() {
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
            final long meanWaitNanos,
            final long seed,
            final double percentNull
    ) {
        if (meanWaitNanos <= 0) {
            throw new IllegalArgumentException(String.format("period_nanos (=%d) should be > 0.", meanWaitNanos));
        }
        return new RandomGenerator(ColumnType.TIMESTAMP_NANOS, seed, percentNull, new PrimitiveIterator.OfLong() {
            final Random prng = new Random(seed);
            final double period = (double) meanWaitNanos;
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
            final double percentNull
    ) {
        if (lambda <= 0) {
            throw new IllegalArgumentException(String.format("lambda (=%g) should be > 0", lambda));
        }
        return new RandomGenerator(ColumnType.DOUBLE, seed, percentNull, new PrimitiveIterator.OfDouble() {
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
            final double percentNull
    ) {
        return new RandomGenerator(ColumnType.INT32, seed, percentNull, new PrimitiveIterator.OfInt() {
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
            final double percentNull
    ) {
        return new RandomGenerator(ColumnType.INT64, seed, percentNull, new PrimitiveIterator.OfLong() {
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
            final double percentNull
    ) {
        return new RandomGenerator(ColumnType.DOUBLE, seed, percentNull, new PrimitiveIterator.OfDouble() {
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

    static int[] accumWeights(final ArrayList<Integer> weights, final int size) {
        final int[] accumWeights = new int[size];
        int accum = 0;
        for (int i = 0; i < size; ++i) {
            final int wi = (weights == null) ? 1 : weights.get(i);
            accum += wi;
            accumWeights[i] = accum;
        }
        return accumWeights;
    }

    static <T> RandomGenerator ofRandomPick(
            final ColumnType columnType,
            final ArrayList<T> options,
            final ArrayList<Integer> weights,
            final long seed,
            final double percentNull
    ) {
        if (weights != null && options.size() != weights.size()) {
            throw new IllegalArgumentException(String.format(
                    "length of options list (=%d) and weights list (%d) should match.",
                    options.size(),
                    weights.size()));
        }
        return new RandomGenerator(columnType, seed, percentNull, new Iterator<T>() {
            final Random prng = new Random(seed);
            final int[] accumWeights = accumWeights(weights, options.size());
            @Override
            public T next() {
                final int weightedPick = prng.nextInt(accumWeights[accumWeights.length - 1] + 1);
                int pos = Arrays.binarySearch(accumWeights, weightedPick);
                if (pos < 0) {
                    pos = ~pos;
                }
                return options.get(pos);
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
            final long seed,
            final double percentNull
    ) {

        switch (columnType) {
            case INT32: {
                final int lowerBound = Utils.getIntElementValue("lower_bound", jo);
                final int upperBound = Utils.getIntElementValue("upper_bound", jo);

                return RandomGenerator.ofUniformInt(columnType, lowerBound, upperBound, seed, percentNull);
            }

            case INT64: {
                final long lowerBound = Utils.getLongElementValue("lower_bound", jo);
                final long upperBound = Utils.getLongElementValue("upper_bound", jo);

                return RandomGenerator.ofUniformLong(columnType, lowerBound, upperBound, seed, percentNull);
            }

            case DOUBLE: {
                final double lowerBound = Utils.getDoubleElementValue("lower_bound", jo);
                final double upperBound = Utils.getDoubleElementValue("upper_bound", jo);

                return RandomGenerator.ofUniformDouble(columnType, lowerBound, upperBound, seed, percentNull);
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
            final long seed,
            final double percentNull
    ) {
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
            final long seed,
            final double percentNull
    ) {
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
            final long seed,
            final double percentNull
    ) {
        final long startNanos = Utils.getLongElementValue("start_nanos", jo);
        final long meanWaitNanos = Utils.getLongElementValue("mean_wait_nanos", jo);
        switch (columnType) {
            case TIMESTAMP_NANOS:
                return RandomGenerator.ofPoissonWait(startNanos, meanWaitNanos, seed, percentNull);
            case INT32:
            case INT64:
            case DOUBLE:
            case STRING:
                throw new IllegalArgumentException(String.format(
                        "%s: output type %s is not supported for poisson_wait distribution",
                        fieldName, columnType));
            default:
                throw new IllegalStateException("Missing column type");
        }
    }

    static RandomGenerator randomWalkFromJson(
            final String fieldName,
            final JSONObject jo,
            final ColumnType columnType,
            final long seed,
            final double percentNull
    ) {
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

    static RandomGenerator randomPickFromJson(final String fieldName,
                                              final JSONObject jo,
                                              final ColumnType columnType,
                                              final long seed,
                                              final double percentNull
    ) {
        switch (columnType) {
            case INT32: {
                final ArrayList<Integer> options = Utils.getIntListElementValues("options", jo);
                final ArrayList<Integer> weights = Utils.getIntListElementValuesOrNull("weights", jo);
                return RandomGenerator.ofRandomPick(columnType, options, weights, seed, percentNull);
            }
            case INT64: {
                final ArrayList<Long> options = Utils.getLongListElementValues("options", jo);
                final ArrayList<Integer> weights = Utils.getIntListElementValuesOrNull("weights", jo);
                return RandomGenerator.ofRandomPick(columnType, options, weights, seed, percentNull);
            }
            case DOUBLE: {
                final ArrayList<Double> options = Utils.getDoubleListElementValues("options", jo);
                final ArrayList<Integer> weights = Utils.getIntListElementValuesOrNull("weights", jo);
                return RandomGenerator.ofRandomPick(columnType, options, weights, seed, percentNull);
            }
            case STRING: {
                final ArrayList<String> options = Utils.getStringListElementValues("options", jo);
                final ArrayList<Integer> weights = Utils.getIntListElementValuesOrNull("weights", jo);
                return RandomGenerator.ofRandomPick(columnType, options, weights, seed, percentNull);
            }
            case TIMESTAMP_NANOS:
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
                return exponentialFromJson(fieldName, jo, columnType, seed, percentNull);
            case "uniform":
                return uniformFromJson(fieldName, jo, columnType, seed, percentNull);
            case "normal":
                return normalFromJson(fieldName, jo, columnType, seed, percentNull);
            case "poisson_wait":
                return poissonWaitFromJson(fieldName, jo, columnType, seed, percentNull);
            case "random_walk":
                return randomWalkFromJson(fieldName, jo, columnType, seed, percentNull);
            case "random_pick":
                return randomPickFromJson(fieldName, jo, columnType, seed, percentNull);
            default:
                throw new IllegalArgumentException(String.format(
                        "Unrecognized value \"%s\" for \"distribution\" element", distribution));
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
            Object o = it.next();

            // even if we end up rolling a null
            if (pctNullMgr.test()) {
                return null;
            }

            return o;
        }
    }
}

