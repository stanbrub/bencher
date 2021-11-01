package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.Random;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

public class RandomGenerator extends DataGenerator {

    private double lower_bound;
    private double upper_bound;
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
        initialize(columnType, (double) lower_bound, (double) upper_bound, percent_null);

        is = prng.ints(lower_bound, upper_bound);
        isi = is.iterator();
    }

    RandomGenerator(ColumnType columnType, double lower_bound, double upper_bound, long seed, double percent_null) {

        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.prng = new Random(seed);
        objectIterator = new GeneratorObjectIterator();
        stringIterator = new GeneratorStringIterator();
        initialize(columnType, lower_bound, upper_bound, percent_null);

        ds = prng.doubles(lower_bound, upper_bound);
        dsi = ds.iterator();
    }

    RandomGenerator(ColumnType columnType, long lower_bound, long upper_bound, long seed, double percent_null) {

        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.prng = new Random(seed);
        objectIterator = new GeneratorObjectIterator();
        stringIterator = new GeneratorStringIterator();
        initialize(columnType, (double) lower_bound, (double) upper_bound, percent_null);

        ls = prng.longs(lower_bound, upper_bound);
        lsi = ls.iterator();
    }

    private void initialize(ColumnType columnType, double lower_bound, double upper_bound, double percent_null) {

        this.columnType = columnType;
        this.lower_bound = lower_bound;
        this.upper_bound = upper_bound;
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

        RandomGenerator rg = null;
        long seed = Long.parseLong((String) jo.get("seed"));


        switch (columnType) {
            case DOUBLE:
            {
                double lower_bound = Double.parseDouble(lower);
                double upper_bound = Double.parseDouble(upper);

                rg = new RandomGenerator(columnType, lower_bound, upper_bound, seed, percent_null);
                break;
            }

            case INT32:
            {
                int lower_bound = Integer.parseInt(lower);
                int upper_bound = Integer.parseInt(upper);

                rg = new RandomGenerator(columnType, lower_bound, upper_bound, seed, percent_null);
                break;
            }

            case INT64:
            {
                long lower_bound = Long.parseLong(lower);
                long upper_bound = Long.parseLong(upper);

                rg = new RandomGenerator(columnType, lower_bound, upper_bound, seed, percent_null);
                break;
            }

            case STRING:
            default:
                throw new IllegalArgumentException(String.format("%s: output type %s is not supported", fieldName, columnType));
        }

        return rg;
    }


    private Object getNext() {

        Object o = null;
        switch (columnType) {

            case DOUBLE:
                o = dsi.next();
                break;

            case INT32:
                o = isi.next();
                break;

            case INT64:
                o = lsi.next();
                break;

            case STRING:
            default:
                throw new InternalError(String.format("column type %s is not supported", columnType));

        }

        return o;
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

