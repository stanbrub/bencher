package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Iterator;
import java.util.Locale;

public class IDGenerator extends DataGenerator {

    private long currentID;
    private final PercentNullManager percent_null;
    private final GeneratorObjectIterator objectIterator;
    private final Increment increment;

    enum Increment {
        INCREASING,
        DECREASING,
    }

    private IDGenerator(
            final ColumnType columnType,
            final long start_id,
            final long seed,
            final Increment increment,
            final double percent_null) {

        this.currentID = start_id;
        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);
        this.columnType = columnType;

        this.increment = increment;

        this.objectIterator = new GeneratorObjectIterator();
    }


    static IDGenerator fromJson(final String fieldName, final JSONObject jo) {
        final ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        switch (columnType) {
            case INT32:
            case INT64:
                break;
            case DOUBLE:
            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException(
                        "Only column types INT32 or INT64 are supported for " +
                                IDGenerator.class.getSimpleName());
            default:
                throw new IllegalStateException("Missing types");
        }

        final Increment increment;
        final String inc = (String) jo.get("increment");
        if (inc == null) {
            System.err.printf("%s: increment not found, defaulting to Increasing\n", fieldName);
            increment = Increment.INCREASING;
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

        final double percent_null = PercentNullManager.parseJson(fieldName, jo);

        final IDGenerator frg = new IDGenerator(
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
            else
                throw new InternalError("Need to implement more types");
        }
    }


    @Override
    public Iterator<Object> getIterator() {
        return objectIterator;
    }

}
