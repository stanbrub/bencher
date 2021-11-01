package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Locale;
import java.util.Random;

/***
 * A io.deephaven.datagen.FullRangeGenerator generates a full range of integer values from a starting number to
 * an ending number, inclusive. The range may be shuffled, increasing, or decreasing.
 *
 * Increasing and decreasing orderings are dynamically generated. Shuffling requires a shuffle -- so an array is
 * built, shuffled, then enumerated. Of course, that takes O(n) for memory and time.
 */
public class FullRangeGenerator extends DataGenerator {

    private long start;
    private long stop;
    private long seed;
    private long current;
    private int capacity;
    private PercentNullManager percent_null;
    private Ordering order;
    private Random prng;
    private ArrayList<Long> deck;
    private Iterator<Long> deckIterator;
    private GeneratorStringIterator stringIterator;
    private GeneratorObjectIterator objectIterator;

    enum Ordering {
        INCREASING,
        DECREASING,
        SHUFFLED,
    }

    private FullRangeGenerator(ColumnType columnType, long start, long stop, long seed, Ordering ordering, double percent_null) {

        if (stop < start)
            throw new IllegalArgumentException(String.format("start %d must be lower than stop %d", start, stop));
        this.start = start;
        this.stop = stop;
        this.seed = seed;
        this.prng = new Random(seed);
        this.order = ordering;
        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);

        this.columnType = columnType;

        initialize();

        stringIterator = new GeneratorStringIterator();
        objectIterator = new GeneratorObjectIterator();
    }

    private void initialize() {

        capacity = (int) (stop - start + 1);

        if (order == Ordering.SHUFFLED) {

            // start with all the integers in order
            deck = new ArrayList<Long>(capacity);
            for (long n = start; n <= stop; ++n) {
                deck.add(n);
            }

            // do we need a shuffle?
            if (order == Ordering.SHUFFLED) {
                for (int idx = 0; idx < capacity - 1; idx++) {

                    int target = prng.nextInt(capacity - idx - 1);

                    Long temp = deck.get(target);
                    deck.set(target, deck.get(idx));
                    deck.set(idx, temp);
                }
            }

            deckIterator = deck.iterator();
        } else if (order == Ordering.INCREASING) {
            current = start;
        } else if (order == Ordering.DECREASING) {
            current = stop;
        } else {
            throw new InternalError(String.format("Not ready to handle ordering %s", order));
        }
    }

    @Override
    int getCapacity() {
        return capacity;
    }

    static FullRangeGenerator fromJson(String fieldName, JSONObject jo) {

        ColumnType columnType = DataGenerator.columnTypeFromJson(jo);

        Ordering order = Ordering.INCREASING;
        String ordering = (String) jo.get("order");
        if (ordering == null) {
            System.err.printf("order not found, defaulting to Increasing\n");
        } else {
            switch (ordering.toUpperCase(Locale.ROOT)) {
                case "INCREASING":
                    order = Ordering.INCREASING;
                    break;

                case "DECREASING":
                    order = Ordering.DECREASING;
                    break;

                case "SHUFFLED":
                    order = Ordering.SHUFFLED;
                    break;

                default:
                    throw new IllegalArgumentException(String.format("Ordering must be one of Increasing, Decreasing, or Shuffled, found \"%s\"", ordering));
            }
        }

        double percent_null = PercentNullManager.parseJson(fieldName, jo);

        FullRangeGenerator frg = new FullRangeGenerator(
                columnType,
                Long.parseLong((String) jo.get("range_start")),
                Long.parseLong((String) jo.get("range_stop")),
                Long.parseLong((String) jo.get("seed")),
                order,
                percent_null
        );

        return frg;
    }

    private boolean generatorHasNext() {

        if (deckIterator != null)
            return deckIterator.hasNext();
        else {
            if (order == Ordering.INCREASING)
                return (current <= stop);
            else if (order == Ordering.DECREASING)
                return (current >= start);
        }

        throw new InternalError();
    }

    private long generatorGetNext() {

        if (deckIterator != null)
            return deckIterator.next();
        else {
            if (order == Ordering.INCREASING)
                return current++;
            else if (order == Ordering.DECREASING)
                return current--;
        }

        throw new InternalError();
    }

    @Override
    public Iterator<String> getStringIterator() {
        return stringIterator;
    }

    @Override
    public Iterator<Object> getObjectIterator() {
        return objectIterator;
    }


    private class GeneratorStringIterator implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return generatorHasNext();
        }

        @Override
        public String next() {
            // consume from the iterator so the count is correct
            long nextItem = generatorGetNext();

            // even if we end up rolling a null
            if (percent_null != null && percent_null.test()) {
                return null;
            }

            return Long.toString(nextItem);
        }
    }

    private class GeneratorObjectIterator implements Iterator<Object> {

        @Override
        public boolean hasNext() {
            return generatorHasNext();
        }

        @Override
        public Object next() {
            // consume from the iterator so the count is correct
            long nextItem = generatorGetNext();

            // even if we end up rolling a null
            if (percent_null != null && percent_null.test()) {
                return null;
            }

            if (columnType == ColumnType.INT32)
                return (int) nextItem;
            else if (columnType == ColumnType.INT64)
                return nextItem;
            else if (columnType == ColumnType.DOUBLE)
                return (double) nextItem;
            else
                throw new InternalError("Need to implement more types");
        }
    }

}


