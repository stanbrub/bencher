package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class RandomGenerator extends DataGenerator {

    private PercentNullManager percent_null;
    private GeneratorObjectIterator objectIterator;
    private GeneratorStringIterator stringIterator;

    private Random prng;
    private DoubleStream ds = null;
    private PrimitiveIterator.OfDouble dsi = null;
    private IntStream is = null;
    private PrimitiveIterator.OfInt isi = null;
    private LongStream ls = null;
    private PrimitiveIterator.OfLong lsi = null;

    RandomGenerator(ColumnType columnType, int lower_bound, int upper_bound, long seed, double percent_null) {

        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.prng = new Random(seed);
        objectIterator = new GeneratorObjectIterator();
        stringIterator = new GeneratorStringIterator();
        this.columnType = columnType;

        is = prng.ints(lower_bound, upper_bound);
        isi = is.iterator();
    }

    RandomGenerator(ColumnType columnType, double lower_bound, double upper_bound, long seed, double percent_null) {

        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.prng = new Random(seed);
        objectIterator = new GeneratorObjectIterator();
        stringIterator = new GeneratorStringIterator();
        this.columnType = columnType;

        ds = prng.doubles(lower_bound, upper_bound);
        dsi = ds.iterator();
    }

    RandomGenerator(ColumnType columnType, long lower_bound, long upper_bound, long seed, double percent_null) {

        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.prng = new Random(seed);
        objectIterator = new GeneratorObjectIterator();
        stringIterator = new GeneratorStringIterator();
        this.columnType = columnType;

        ls = prng.longs(lower_bound, upper_bound);
        lsi = ls.iterator();
    }

    @Override
    int getCapacity() {
        return -1;
    }

    @Override
    public Iterator<String> getStringIterator() {
        return stringIterator;
    }

    @Override
    public Iterator<Object> getObjectIterator() {
        return objectIterator;
    }


    static RandomGenerator fromJson(String fieldName, JSONObject jo) {

        ColumnType columnType = DataGenerator.columnTypeFromJson(jo);

        double percent_null = PercentNullManager.parseJson(fieldName, jo);

        String lower = (String) jo.get("lower_bound");
        String upper = (String) jo.get("upper_bound");

        long seed = Long.parseLong((String) jo.get("seed"));

        switch (columnType) {
            case DOUBLE:
            {
                double lower_bound = Double.parseDouble(lower);
                double upper_bound = Double.parseDouble(upper);

                return new RandomGenerator(columnType, lower_bound, upper_bound, seed, percent_null);
            }

            case INT32:
            {
                int lower_bound = Integer.parseInt(lower);
                int upper_bound = Integer.parseInt(upper);

                return new RandomGenerator(columnType, lower_bound, upper_bound, seed, percent_null);
            }

            case INT64:
            {
                long lower_bound = Long.parseLong(lower);
                long upper_bound = Long.parseLong(upper);

                return new RandomGenerator(columnType, lower_bound, upper_bound, seed, percent_null);
            }

            case STRING:
            default:
                throw new IllegalArgumentException(String.format("%s: output type %s is not supported", fieldName, columnType));
        }
    }


    private Object getNext() {
        switch (columnType) {

            case DOUBLE:
                return dsi.next();

            case INT32:
                return isi.next();

            case INT64:
                return lsi.next();

            case STRING:
            default:
                throw new InternalError(String.format("column type %s is not supported", columnType));
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

    private class GeneratorStringIterator implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {

            // consume from the iterator ...
            Object o = getNext();

            // even if we end up rolling a null
            if (percent_null != null && percent_null.test()) {
                return null;
            }

            return o.toString();
        }
    }


}

