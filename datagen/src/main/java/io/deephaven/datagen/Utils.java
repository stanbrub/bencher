package io.deephaven.datagen;

import org.json.simple.JSONObject;
import org.mortbay.util.ajax.JSON;

import java.io.*;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.function.Function;

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

    private static String getStringElementValueMaybeDefault(
        final String key, final JSONObject jo, final boolean haveDefault, final String defaultValue) {
        final Object jsonValue = jo.get(key);
        if (jsonValue == null) {
            if (haveDefault) {
                return defaultValue;
            } else {
                throw new IllegalArgumentException(String.format("Missing \"%s\" element", key));
            }
        }
        if (!(jsonValue instanceof String)) {
            throw new IllegalArgumentException(String.format("Wrong type for \"%s\" element, should be string", key));
        }
        return (String) jsonValue;
    }

    public static String getStringElementValue(final String key, final JSONObject jo) {
        return getStringElementValueMaybeDefault(key, jo, false, null);
    }

    public static String getStringElementValueOrDefault(final String key, final JSONObject jo, final String defaultValue) {
        return getStringElementValueMaybeDefault(key, jo, true, defaultValue);
    }

    public static int getIntElementValue(final String key, final JSONObject jo) {
        return getIntElementValueMaybeDefault(key, jo, false, 0);
    }

    public static int getIntElementValueOrDefault(final String key, final JSONObject jo, final int defaultValue) {
        return getIntElementValueMaybeDefault(key, jo, true, defaultValue);
    }

    public static boolean getBooleanElementValueOrDefault(final String key, final JSONObject jo, final boolean defaultValue) {
        return getBooleanElementValueMaybeDefault(key, jo, true, defaultValue);
    }

    private static int getIntElementValueMaybeDefault(
            final String key, final JSONObject jo, final boolean haveDefault, final int defaultValue) {
        final Object jsonValue = jo.get(key);
        if (jsonValue == null) {
            if (haveDefault) {
                return defaultValue;
            } else {
                throw new IllegalArgumentException(String.format("Missing \"%s\" element", key));
            }
        }
        if (!(jsonValue instanceof String)) {
            throw new IllegalArgumentException(String.format("Wrong type for \"%s\" element, should be string", key));
        }
        final String value = (String) jsonValue;
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException ex) {
            throw new IllegalArgumentException(
                    String.format("Couldn't parse value \"%s\" for element \"%s\" as an int.", value, key),
                    ex);
        }
    }

    private static boolean getBooleanElementValueMaybeDefault(
            final String key, final JSONObject jo, final boolean haveDefault, final boolean defaultValue) {
        final Object jsonValue = jo.get(key);
        if (jsonValue == null) {
            if (haveDefault) {
                return defaultValue;
            } else {
                throw new IllegalArgumentException(String.format("Missing \"%s\" element", key));
            }
        }
        if (!(jsonValue instanceof String)) {
            throw new IllegalArgumentException(String.format("Wrong type for \"%s\" element, should be string", key));
        }
        final String value = (String) jsonValue;
        if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("t") || value.equalsIgnoreCase("1")) {
            return true;
        }
        if (value.equalsIgnoreCase("false") || value.equalsIgnoreCase("f") || value.equalsIgnoreCase("0")) {
            return false;
        }
        throw new IllegalArgumentException(String.format("Couldn't parse value \"%s\" for element \"%s\" as a boolean.", value, key));
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
            System.err.printf("IOException while reading %s: %s", filename, e);
            throw new IllegalStateException();
        }

        return values;
    }

    public static ArrayList<String> getStringListElementValues(final String key, final JSONObject jo) {
        return getStringListElementValues(key, jo, false);
    }

    public static ArrayList<String> getStringListElementValuesOrNull(final String key, final JSONObject jo) {
        return getStringListElementValues(key, jo, true);
    }

    private static ArrayList<String> getStringListElementValues(final String key, final JSONObject jo, final boolean allowNull) {
        final Object valuesObj =  jo.get(key);
        if (valuesObj == null) {
            if (allowNull) {
                return null;
            }
            throw new IllegalArgumentException(String.format("Element \"%s\" should exist.", key));
        }
        if (!(valuesObj instanceof ArrayList)) {
            throw new IllegalArgumentException(String.format("Element \"%s\" should be a list.", key));
        }

        final ArrayList<?> list = (ArrayList<?>) valuesObj;
        for (int i = 0; i < list.size(); ++i) {
            final Object o = list.get(i);
            if (!(o instanceof String)) {
                throw new IllegalArgumentException(String.format("Element in position %d (=\"%s\") is not a string.", i+1, o.toString()));
            }
        }
        return (ArrayList<String>) list;
    }

    public static ArrayList<Integer> getIntListElementValues(final String key, final JSONObject jo) {
        return getListElementValues(Integer::parseInt, key, jo, false);
    }

    public static ArrayList<Integer> getIntListElementValuesOrNull(final String key, final JSONObject jo) {
        return getListElementValues(Integer::parseInt, key, jo, true);
    }

    public static ArrayList<Long> getLongListElementValues(final String key, final JSONObject jo) {
        return getListElementValues(Long::parseLong, key, jo, false);
    }

    public static ArrayList<Long> getLongListElementValuesOrNull(final String key, final JSONObject jo) {
        return getListElementValues(Long::parseLong, key, jo, true);
    }

    public static ArrayList<Double> getDoubleListElementValues(final String key, final JSONObject jo) {
        return getListElementValues(Double::parseDouble, key, jo, false);
    }

    public static ArrayList<Double> getDoubleListElementValuesOrNull(final String key, final JSONObject jo) {
        return getListElementValues(Double::parseDouble, key, jo, true);
    }

    private static <T> ArrayList<T> getListElementValues(Function<String, T> fromString, final String key, final JSONObject jo, final boolean allowNull) {
        final ArrayList<String> list = getStringListElementValues(key, jo, allowNull);
        if (list == null) {
            return null;
        }
        final ArrayList<T> out = new ArrayList<>(list.size());
        for (int i = 0; i < list.size(); ++i) {
            final String s = list.get(i);
            final T v;
            try {
                v = fromString.apply(s);
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(
                        String.format("Element in position %d (=\"%s\" cannot be converted to int", i+1, s), ex);
            }
            out.add(v);
        }
        return out;
    }

    public static ArrayList<?> getColumnTypeElementValues(final DataGenerator.ColumnType columnType, final String key, final JSONObject jo) {
        switch (columnType) {
            case INT32:
                return getIntListElementValues(key, jo);
            case INT64:
                return getLongListElementValues(key, jo);
            case DOUBLE:
                return getDoubleListElementValues(key, jo);
            case STRING:
                return getStringListElementValues(key, jo);
            case TIMESTAMP_NANOS:
                throw new IllegalArgumentException("Unsupported type " + columnType);
            default:
                throw new IllegalStateException("Missing types");
        }
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
