package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;


public class FileGenerator extends DataGenerator {

    PercentNullManager percent_null;
    ArrayList<String> strings;
    String fileName;
    GeneratorStringIterator stringIterator;
    GeneratorObjectIterator objectIterator;
    int currentIndex;

    private FileGenerator(ColumnType columnType, String fileName, long seed, double percent_null) {

        this.columnType = columnType;

        this.fileName = fileName;

        this.percent_null = PercentNullManager.fromPercentage(percent_null, seed);

        initialize();

        stringIterator = new GeneratorStringIterator();
        objectIterator = new GeneratorObjectIterator();
    }

    private void initialize() {

        strings = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(fileName))) {
            while (br.ready()) {
                strings.add(br.readLine());
            }
        } catch (FileNotFoundException e) {
            System.err.printf("File %s not found", fileName);
            throw new IllegalStateException();
        } catch (IOException e) {
            System.err.printf("IOException while reading %s: %s", fileName, e.getMessage());
            throw new IllegalStateException();
        }

        currentIndex = 0;
    }

    @Override
    int getCapacity() {
        return strings.size();
    }

    static DataGenerator fromJson(String fieldName, JSONObject jo) {

        ColumnType columnType = DataGenerator.columnTypeFromJson(jo);

        String fileName = (String) jo.get("source_file");
        if (fileName == null) {
            throw new IllegalArgumentException(String.format("%s: source_file must be specified", fieldName));
        }

        String seedValue = (String) jo.get("seed");
        if (seedValue == null) {
            throw new IllegalArgumentException(String.format("%s: Seed must be provided", fieldName));
        }
        long seed = Long.parseLong(seedValue);

        double percent_null = PercentNullManager.parseJson(fieldName, jo);

        FileGenerator sg = new FileGenerator(columnType, fileName, seed, percent_null);
        return sg;
    }

    private boolean generatorHasNext() {

        return currentIndex < strings.size();
    }

    private String generatorGetNext() {

        return strings.get(currentIndex++);
    }

    class GeneratorStringIterator  implements Iterator<String> {

        @Override
        public boolean hasNext() {
            return generatorHasNext();
        }

        @Override
        public String next() {
            // generate something
            String str = generatorGetNext();

            // but possibly discard it
            if (percent_null != null && percent_null.test()) {
                return "";
            }

            return str;
        }
    }

    class GeneratorObjectIterator  implements Iterator<Object> {

        @Override
        public boolean hasNext() {
            return generatorHasNext();
        }

        @Override
        public Object next() {
            // generate something
            String str = generatorGetNext();

            // but possibly discard it
            if (percent_null != null && percent_null.test()) {
                return "";
            }

            if (columnType == ColumnType.INT32)
                return Integer.parseInt(str);
            else if (columnType == ColumnType.INT64)
                return str;
            else if (columnType == ColumnType.DOUBLE)
                return Double.parseDouble(str);
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
