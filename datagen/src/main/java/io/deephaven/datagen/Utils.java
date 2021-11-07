package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.io.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;

public class Utils {
    public static File locateFile(final File dir, final String generatorFilename) {
        if (generatorFilename.startsWith(File.separator)) {
            final File file = new File(generatorFilename);
            if (!file.exists()) {
                throw new IllegalArgumentException("Couldn't find file " + file.getAbsolutePath());
            }
            return file;
        }
        String generatorAbsolutePath = dir.getAbsolutePath() + File.separator + generatorFilename;
        File file = new File(generatorAbsolutePath);
        if (!file.exists()) {
            generatorAbsolutePath = dir.getParent() + File.separator + generatorFilename;
            file = new File(generatorAbsolutePath);
            if (!file.exists()) {
                throw new IllegalArgumentException(
                        "Couldn't find file \"" + generatorFilename + "\" in \"" + dir.getPath() + "\" or its parent.");
            }
        }
        return file;
    }

    public static String getStringElementValue(final String key, final JSONObject jo) {
        final Object jsonValue = jo.get(key);
        if (jsonValue == null) {
            throw new IllegalArgumentException(String.format("Missing \"%s\" element", key));
        } else if (!(jsonValue instanceof String)) {
            throw new IllegalArgumentException(String.format("Wrong type for \"%s\" element, should be string", key));
        }
        return (String) jsonValue;
    }

    public static int getIntElementValue(final String key, final JSONObject jo) {
        final String value = getStringElementValue(key, jo);
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Couldn't parse value \"%s\" for element \"%s\" as an int.", value, key),
                    ex);
        }
    }

    public static long getLongElementValue(final String key, final JSONObject jo) {
        final String value = getStringElementValue(key, jo);
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Couldn't parse value \"%s\" for element \"%s\" as a long.", value, key),
                    ex);
        }
    }

    public static double getDoubleElementValue(final String key, final JSONObject jo) {
        final String value = getStringElementValue(key, jo);
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Couldn't parse value \"%s\" for element \"%s\" as a double.", value, key),
                    ex);
        }
    }

    public static ArrayList<Object> readFile(final String filename, final DataGenerator.ColumnType columnType) {
        final ArrayList<Object> values = new ArrayList<>();
        final File file = new File(filename);
        try (BufferedReader br = new BufferedReader(new FileReader(file))) {
            int line = 0;
            while (br.ready()) {
                final String strVal = br.readLine();
                ++line;
                final Object value;
                try {
                    value = Utils.stringValueAsType(strVal, columnType);
                } catch (Exception ex) {
                    throw new IllegalArgumentException(
                            String.format("Error in file \"%s\" line %d", file.getAbsolutePath(), line),
                            ex);
                }
                values.add(value);
            }
        } catch (FileNotFoundException e) {
            System.err.printf("File %s not found", filename);
            throw new IllegalStateException();
        } catch (IOException e) {
            System.err.printf("IOException while reading %s: %s", filename, e.getMessage());
            throw new IllegalStateException();
        }

        return values;
    }

    public static Object stringValueAsType(
            final String strValue,
            final DataGenerator.ColumnType columnType) {
        switch (columnType) {
            case INT32:
                return Integer.parseInt(strValue);
            case INT64:
                return Long.parseLong(strValue);
            case DOUBLE:
                return Double.parseDouble(strValue);
            case STRING:
                return strValue;
            case TIMESTAMP_NANOS: {
                final TemporalAccessor t = DateTimeFormatter.ISO_DATE_TIME.parse(strValue);
                final long nanos =
                        1000 * 1000 * 1000 * t.getLong(ChronoField.INSTANT_SECONDS) +
                                t.get(ChronoField.NANO_OF_SECOND);
                return new DataGenerator.UnixTimestampNanos(nanos);
            }
            default:
                throw new IllegalStateException("Need to implement more types");
        }
    }
}
