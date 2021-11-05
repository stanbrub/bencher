package io.deephaven.datagen;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// https://github.com/fangyidong/json-simple
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

public class DataGen {

    private enum OutputFormat {
        PARQUET,
        CSV,
    }

    private static final boolean OVERWRITE = Boolean.parseBoolean(System.getProperty("data.overwrite", "true"));

    /***
     * Construct an io.deephaven.datagen.CustomParquetWriter given the MessageType schema that we're passed.
     *
     * @param customWriterSupport    CustomWriterSupport for the schema we will be writing.
     * @return          io.deephaven.datagen.CustomParquetWriter initialized with a random file name, ready to write
     * @throws IOException
     */
    private static CustomParquetWriter getParquetWriter(final String outputFilePath, CustomWriterSupport customWriterSupport) throws IOException {

        File outputParquetFile = new File(outputFilePath);
        if (outputParquetFile.exists() && OVERWRITE) {
            outputParquetFile.delete();
        }
        Path path = new Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(
                path, customWriterSupport, false, CompressionCodecName.ZSTD
        );
    }

    /***
     * Parse the output format from the given JSON document map
     *
     * @param document      JSON document map, positioned at top-level
     * @return              OutputFormat, or an exception about a bad type
     */
    private static OutputFormat getOutputFormat(final Map<String, Object> document) {
        final String fmt = (String) document.get("format");
        if (fmt == null) {
            System.out.println("no format found, defaulting to PARQUET");
            return OutputFormat.PARQUET;
        }

        try {
            return Enum.valueOf(OutputFormat.class, fmt);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(String.format("unrecognized output format \"%s\"", fmt));
        }
    }

    private static String getOutputFilename(final String generatorFilename, final OutputFormat format) {
        final String basename = strip(generatorFilename);
        final String extension;
        switch (format) {
            case CSV:
                extension = "csv";
                break;
            case PARQUET:
                extension = "parquet";
                break;
            default:
                throw new IllegalStateException("unrecognized format " + format);
        }
        final String prefix = System.getProperty("output.prefix.path", ".");
        return prefix + File.separator + basename + "." + extension;
    }

    /***
     * Generates a Parquet-format file given the list of generators. This function exhausts
     * the generators and then closes the file.
     *
     * @param outputFileName  Filename to write output to.
     * @param generators    Map of generators, one for each column we expect to write.
     * @throws IOException
     */
    private static void generateParquet(
            final String outputFileName,
            final Map<String, DataGenerator> generators) throws IOException {

        // build typed Parquet structure
        // need a MessageTypeBuilder so we can create the protobuf type that Parquet uses
        Types.MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
        for (Map.Entry<String, DataGenerator> entry : generators.entrySet()) {

            final DataGenerator dg = entry.getValue();
            final String fieldName = entry.getKey();

            builder.addField(DataGenerator.parquetTypeFromJSONType(dg.getColumnType(), entry.getKey()));
        }

        MessageType mt = builder.named("MyMessage");

        final CustomWriterSupport customWriterSupport = new CustomWriterSupport(mt);
        CustomParquetWriter pqw2 = getParquetWriter(outputFileName, customWriterSupport);

        final Set<Map.Entry<String, DataGenerator>> entrySet = generators.entrySet();
        final Object[] data = new Object[entrySet.size()];
        boolean more = true;
        while (more) {
            int i = 0;
            for (Map.Entry<String, DataGenerator> entry : entrySet) {
                DataGenerator gen = entry.getValue();
                final Iterator<Object> iter = gen.getIterator();
                if (!iter.hasNext()) {
                    more = false;
                    break;
                }
                data[i++] = iter.next();
            }
            if (more) {
                pqw2.write(data);
            }
        }

        customWriterSupport.flush();
        pqw2.close();
    }

    /***
     * Generates a CSV file from the list of generators. This function will exhaust the
     * generators. Also writes a header at the first row, using the column names.
     *
     * @param generators    Map of generators, one for each column we expect to write.
     */
    private static void generateCSV(Map<String, DataGenerator> generators, FileWriter outputFile) throws IOException {

        StringBuilder headerBuilder = new StringBuilder();
        boolean first = true;

        for (Map.Entry<String, DataGenerator> entry : generators.entrySet()) {

            if (!first) {
                headerBuilder.append(',');
            } else {
                first = false;
            }

            headerBuilder.append(entry.getKey());
        }
        outputFile.write(headerBuilder.toString());
        outputFile.append('\n');

        for (boolean more = true; more; /* inside */ ) {
            final StringBuilder rowBuilder = new StringBuilder();
            first = true;
            for (Map.Entry<String, DataGenerator> entry : generators.entrySet()) {
                final String fieldName = entry.getKey();
                final DataGenerator gen = entry.getValue();

                if (!gen.getIterator().hasNext()) {
                    more = false;
                    break;
                }

                final Object val = gen.getIterator().next();
                if (!first) {
                    rowBuilder.append(',');
                } else {
                    first = false;
                }

                if (val != null)
                    rowBuilder.append(val);
            }

            if (more) {
                outputFile.write(rowBuilder.toString());
                outputFile.append('\n');
            }
        }
    }

    /** Strip any extension and base directory. */
    private static String strip(final String filename) {
        if (filename.startsWith(".")) {
            throw new IllegalArgumentException("filename can't start with \".\"");
        }
        final int basenameIndex = filename.lastIndexOf(File.separator);
        final int start = basenameIndex + 1;
        final int extensionIndex = filename.indexOf(".");
        final int end = (extensionIndex == -1) ? filename.length() : extensionIndex;
        return filename.substring(start, end);
    }

    /**
     * generate test data by reading the given JSON file and following the directives within
     *
     * @param dir                        A directory relative to which interpret the generatorFilename.
     * @param generatorFilename          String with the generatorFilename to be read
     * @throws IOException
     * @throws ParseException
     */
    public static void generateData(final File dir, final String generatorFilename) throws IOException, ParseException {
        final File generatorFile;
        if (!generatorFilename.startsWith(File.separator)) {
            generatorFile = Utils.locateFile(dir, generatorFilename);
        } else {
            generatorFile = new File(generatorFilename);
            if (!generatorFile.exists()) {
                throw new IllegalArgumentException("Generator file \"" + generatorFilename + "\" doesn't exist");
            }
        }
        // get the JSON file root object as a map of column names to JSON objects
        final JSONObject jsonMap = (JSONObject) new JSONParser().parse(new FileReader(generatorFile));
        final Map<String, Object> documentDictionary = (Map<String, Object>) jsonMap;
        final OutputFormat format = getOutputFormat(documentDictionary);
        final String outputFilename = getOutputFilename(generatorFilename, format);

        final boolean forceGeneration = Boolean.parseBoolean(System.getProperty("force.generation", "False"));
        if (!forceGeneration) {
            final File outputFile = new File(outputFilename);
            if (outputFile.exists() && outputFile.lastModified() > generatorFile.lastModified()) {
                System.out.println("Not generating " + outputFile.getAbsolutePath() +
                        " since it exists and is older than " + generatorFile.getAbsolutePath());
                return;
            }
        }
        System.out.println("Generating " + outputFilename + ".");

        final Map<String, Object> columnDictionary = (Map<String, Object>) documentDictionary.get("columns");

        // map from string (name of column) to our io.deephaven.datagen.DataGenerator-derived objects
        final Map<String, DataGenerator> generators = new TreeMap<>();

        // for each entry in the JSON document ...
        for (Map.Entry<String, Object> entry : columnDictionary.entrySet()) {

            // get the column name and the JSON object
            String fieldName = entry.getKey();
            JSONObject jsonField = (JSONObject) entry.getValue();

            // build a JSON object
            String generation_type = (String) jsonField.get("generation_type");
            // System.err.printf("%s: %s\n", fieldName, generation_type);

            // create that object and dump it into the map
            DataGenerator gen = DataGenerator.fromJson(fieldName, jsonField);
            generators.put(fieldName, gen);
        }

        if (format == OutputFormat.PARQUET) {
            generateParquet(outputFilename, generators);
        } else if (format == OutputFormat.CSV) {

            final FileWriter outputFile;
            try {
                outputFile = new FileWriter(outputFilename);
            } catch (IOException ex) {
                String err = String.format("Couldn't create output file: %s\n", ex.getMessage());
                System.err.printf(err);
                throw new InternalError(err);
            }

            generateCSV(generators, outputFile);
            outputFile.close();
        } else {
            throw new InternalError(String.format("Not ready to handle format %s", format));
        }
    }

}
