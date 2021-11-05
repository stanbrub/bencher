package io.deephaven.datagen;

import org.json.simple.JSONObject;

import java.util.Random;

/**
 * Helper class that manages generation of Null values. The class wraps a PRNG and a
 * percentage; it can be called to roll for nullness.
 */
public class PercentNullManager {

    private Random prng_nulls = null;
    private double percent_null;


    /**
     * Returns an initialized PercentNullManager given a percentage and a seed.
     *
     * @param percent_null      double percentage of producing nulls, in the range [0..100];
     *                          represented as a percentage (50) not a fraction (0.50)
     * @param seed              long with the seed to use for this PRNG
     * @return  an initialized PercentNullManager
     */
    static PercentNullManager fromPercentage(double percent_null, long seed) {
        if (percent_null == 0.0)
            return null;

        return new PercentNullManager(percent_null, seed);
    }

    /**
     * Constructs a PercentNullManager given a percentage and a seed.
     *
     * @param percent_null      double percentage of producing nulls, in the range [0..100];
     *                          represented as a percentage (50) not a fraction (0.50)
     * @param seed              long with the seed to use for this PRNG
     */
    PercentNullManager(double percent_null, long seed) {

        if (percent_null > 100 || percent_null < 0)
            throw new IllegalArgumentException("percent_null must be between 0 and 100");

        this.percent_null = percent_null;
        prng_nulls = new Random(seed);
    }

    /**
     * Reads a percentage from JSON to help initialize a PercentNullManager.
     *
     * @param fieldName     String with the field name we're working; just for error messages
     * @param jo            JSONObject set to the JSON representation for this field
     * @return              a double indicating our percent changes
     */
    public static double parseJson(String fieldName, JSONObject jo) {

        double percent_null = 0.0;
        String pct_null = (String) jo.get("percent_null");
        if (pct_null == null) {
            System.err.printf("%s: defaulting to no null values\n", fieldName);
        } else {
            try {
                percent_null = Double.parseDouble(pct_null) / 100.0;
            } catch (NumberFormatException ex) {
                throw new IllegalArgumentException(String.format("percent_null must be a float; read \"%s\"", pct_null));
            }

            if (percent_null > 100 || percent_null < 0) {
                throw new IllegalArgumentException(String.format("percent_null must be between 0 and 100 inclusive; read \"%s\"", pct_null));
            }
        }

        return percent_null;
    }


    /**
     * Rolls for nullness using the initalized rules.
     * @return  true if a null should be generated, false otherwise.
     */
    public boolean test() {

        if (prng_nulls != null && prng_nulls.nextDouble() < percent_null) {
            return true;
        }

        return false;
    }
}
