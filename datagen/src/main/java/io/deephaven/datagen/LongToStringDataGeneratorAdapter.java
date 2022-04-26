package io.deephaven.datagen;

import java.util.Iterator;

public class LongToStringDataGeneratorAdapter extends DataGenerator {
    final DataGenerator wrappedGenerator;
    final boolean hex;

    public LongToStringDataGeneratorAdapter(DataGenerator wrappedGenerator, boolean hex) {
        super(ColumnType.STRING);
        this.wrappedGenerator = wrappedGenerator;
        if (wrappedGenerator.getColumnType() != ColumnType.INT64) {
            throw new IllegalArgumentException("Wrapped generator must produce INT64, but produces " + wrappedGenerator.getColumnType());
        }
        this.hex = hex;
    }

    @Override
    public Iterator<Object> getIterator() {
        final Iterator<Object> wrapped = wrappedGenerator.getIterator();

        return new Iterator<Object>() {
            @Override
            public boolean hasNext() {
                return wrapped.hasNext();
            }

            @Override
            public Object next() {
                Long next = (Long)wrapped.next();
                if (next == null) {
                    return next;
                }

                if (hex) {
                    return Long.toHexString(next);
                } else {
                    return Long.toString(next);
                }
            }
        };
    }
}
