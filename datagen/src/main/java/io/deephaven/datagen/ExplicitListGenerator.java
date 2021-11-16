package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;

public class ExplicitListGenerator extends DataGenerator {

    private final PercentNullManager pctNullMgr;
    private final ArrayList<?> values;
    private final int count;
    private final GeneratorObjectIterator objectIterator;
    private int currentIndex;

    private ExplicitListGenerator(
            final ColumnType columnType,
            final ArrayList<?> values,
            final int count,
            final long seed,
            final double pctNullMgr
    ) {
        this.columnType = columnType;
        this.pctNullMgr = PercentNullManager.fromPercentage(pctNullMgr, seed);
        this.values = values;
        this.count = count;

        currentIndex = 0;
        objectIterator = new GeneratorObjectIterator();
    }

    static DataGenerator fromJsonFileGenerator(final String fieldName, final JSONObject jo) {
        final ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        final String filename = Utils.getStringElementValue("source_file", jo);
        final ArrayList<?> values = Utils.readFile(filename, columnType);
        final int count = Utils.getIntElementValueOrDefault("count", jo, -1);

        final long seed = Utils.getLongElementValue("seed", jo);

        final double percent_null = PercentNullManager.parseJson(fieldName, jo);
        final ExplicitListGenerator sg = new ExplicitListGenerator(
                columnType, values, (count != -1) ? count : values.size(), seed, percent_null);
        return sg;
    }

    static DataGenerator fromJsonListGenerator(final String fieldName, final JSONObject jo) {
        final ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        final ArrayList<?> values = Utils.getColumnTypeElementValues(columnType,"values", jo);
        final int count = Utils.getIntElementValueOrDefault("count", jo, -1);
        final long seed = Utils.getLongElementValue("seed", jo);
        final double percent_null = PercentNullManager.parseJson(fieldName, jo);
        final ExplicitListGenerator sg = new ExplicitListGenerator(
                columnType, values, (count != -1) ? count : values.size(), seed, percent_null);
        return sg;
    }

    private boolean generatorHasNext() {
        return currentIndex < count;
    }

    private Object generatorGetNext() {
        return values.get(currentIndex++ % values.size());
    }

    class GeneratorObjectIterator  implements Iterator<Object> {
        @Override
        public boolean hasNext() {
            return generatorHasNext();
        }

        @Override
        public Object next() {
            // generate something
            final Object value = generatorGetNext();

            // but possibly discard it
            if (pctNullMgr.test()) {
                return null;
            }

            return value;
        }
    }

    @Override
    public Iterator<Object> getIterator() {
        return objectIterator;
    }
}
