package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.io.File;

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
}
