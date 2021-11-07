package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.ArrayList;
import java.util.Iterator;

public class ExplicitListGenerator extends DataGenerator {

    final PercentNullManager pctNullMgr;
    final ArrayList<Object> values;
    final GeneratorObjectIterator objectIterator;
    int currentIndex;

    private ExplicitListGenerator(final ColumnType columnType, final ArrayList<Object> values, final long seed, final double pctNullMgr) {
        this.columnType = columnType;
        this.pctNullMgr = PercentNullManager.fromPercentage(pctNullMgr, seed);
        this.values = values;

        currentIndex = 0;
        objectIterator = new GeneratorObjectIterator();
    }

    static DataGenerator fromJsonFileGenerator(String fieldName, JSONObject jo) {

        final ColumnType columnType = DataGenerator.columnTypeFromJson(jo);
        final String filename = Utils.getStringElementValue("source_file", jo);
        final ArrayList<Object> values = Utils.readFile(filename, columnType);

        final long seed = Utils.getLongElementValue("seed", jo);

        final double percent_null = PercentNullManager.parseJson(fieldName, jo);
        final ExplicitListGenerator sg = new ExplicitListGenerator(columnType, values, seed, percent_null);
        return sg;
    }

    private boolean generatorHasNext() {
        return currentIndex < values.size();
    }

    private Object generatorGetNext() {
        return values.get(currentIndex++);
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
