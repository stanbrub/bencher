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

    private final long start;
    private final long stop;
    private long current;
    private final PercentNullManager pctNullMgr;
    private final Ordering order;
    private final Random prng;
    private Iterator<Long> deckIterator;
    private final GeneratorObjectIterator objectIterator;

    enum Ordering {
        INCREASING,
        DECREASING,
        SHUFFLED,
    }

    private FullRangeGenerator(
            final ColumnType columnType,
            final long start,
            final long stop,
            final long seed,
            final Ordering ordering,
            final double pctNullMgr
    ) {
        if (stop < start)
            throw new IllegalArgumentException(String.format("start %d must be lower than stop %d", start, stop));
        this.start = start;
        this.stop = stop;
        this.prng = new Random(seed);
        this.order = ordering;
        this.pctNullMgr = PercentNullManager.fromPercentage(pctNullMgr, seed);

        this.columnType = columnType;

        initialize();

        objectIterator = new GeneratorObjectIterator();
    }

    private void initialize() {
        int capacity = (int) (stop - start + 1);

        if (order == Ordering.SHUFFLED) {

            // start with all the integers in order
            ArrayList<Long> deck = new ArrayList<Long>(capacity);
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

    static FullRangeGenerator fromJson(final String fieldName, final JSONObject jo) {
        ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        switch (columnType) {
            case DOUBLE:
            case INT32:
            case INT64:
                break;
            case STRING:
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException(
                        "Only column types DOUBLE, INT32 or INT64 are supported for " +
                                FullRangeGenerator.class.getSimpleName());
            default:
                throw new IllegalStateException("Missing types");
        }

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
    public Iterator<Object> getIterator() {
        return objectIterator;
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
            if (pctNullMgr.test()) {
                return null;
            }

            switch (columnType) {
                case INT32:
                    return (int) nextItem;
                case INT64:
                    return nextItem;
                case DOUBLE:
                    return (double) nextItem;
                default:
                    throw new IllegalStateException("Need to implement more types");
            }
        }
    }

}


