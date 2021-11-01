package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.Locale;

public class IDGenerator extends DataGenerator {

    private long currentID;
    private PercentNullManager percent_null;
    private GeneratorStringIterator stringIterator;
    private GeneratorObjectIterator objectIterator;
    private Increment increment;

    enum Increment {
        INCREASING,
        DECREASING,
    }

    private IDGenerator(ColumnType columnType, long start_id, long seed, Increment increment, double percent_null) {

        this.currentID = start_id;
        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.columnType = columnType;

        this.increment = increment;

        this.stringIterator = new GeneratorStringIterator();
        this.objectIterator = new GeneratorObjectIterator();
    }


    @Override
    int getCapacity() {
        return -1;
    }

    static IDGenerator fromJson(String fieldName, JSONObject jo) {

        ColumnType columnType = DataGenerator.columnTypeFromJson(jo);

        Increment increment = Increment.INCREASING;
        String inc = (String) jo.get("increment");
        if (inc == null) {
            System.err.printf("%s: increment not found, defaulting to Increasing\n", fieldName);
        } else {
            switch (inc.toUpperCase(Locale.ROOT)) {
                case "INCREASING":
                    increment = Increment.INCREASING;
                    break;

                case "DECREASING":
                    increment = Increment.DECREASING;
                    break;

                default:
                    throw new IllegalArgumentException(String.format("Ordering must be one of Increasing, Decreasing, or Shuffled, found \"%s\"", inc));
            }
        }

        double percent_null = PercentNullManager.parseJson(fieldName, jo);

        IDGenerator frg = new IDGenerator(
                columnType,
                Long.parseLong((String) jo.get("start_id")),
                Long.parseLong((String) jo.get("seed")),
                increment,
                percent_null
        );

        return frg;
    }

    private long getNext() {

        long next = currentID;

        switch (increment) {
            case DECREASING:
                --currentID;
                break;

            case INCREASING:
                ++currentID;
                break;

            default:
                throw new InternalError("Not ready for new increment type");
        }

        return next;
    }

    private class GeneratorStringIterator implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public String next() {

            // consume from the iterator so the count is correct
            long next = getNext();

            // even if we end up rolling a null
            if (percent_null != null && percent_null.test()) {
                return null;
            }

            return Long.toString(next);
        }
    }


    private class GeneratorObjectIterator implements Iterator<Object> {

        @Override
        public boolean hasNext() {
            return true;
        }

        @Override
        public Object next() {

            // consume from the iterator so the count is correct
            long next = getNext();

            // even if we end up rolling a null
            if (percent_null != null && percent_null.test()) {
                return null;
            }

            if (columnType == ColumnType.INT32)
                return (int) next;
            else if (columnType == ColumnType.INT64)
                return Long.toString(next);
            else if (columnType == ColumnType.DOUBLE)
                return (double) next;
            else
                throw new InternalError("Need to implement more types");
        }
    }


    @Override
    public Iterator<String> getStringIterator() {
        return stringIterator;
    }

    @Override
    public Iterator<Object> getObjectIterator() {
        return objectIterator;
    }

}
