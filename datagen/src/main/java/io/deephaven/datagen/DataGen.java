
package io.deephaven.datagen;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

// https://github.com/fangyidong/json-simple
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;

import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

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
     * @param schema    MessageType with the schema we'll be writing
     * @return          io.deephaven.datagen.CustomParquetWriter initialized with a random file name, ready to write
     * @throws IOException
     */
    private static CustomParquetWriter getParquetWriter(final String outputFilePath, MessageType schema) throws IOException {

        File outputParquetFile = new File(outputFilePath);
        if (outputParquetFile.exists() && OVERWRITE) {
            outputParquetFile.delete();
        }
        Path path = new Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(
                path, schema, false, CompressionCodecName.ZSTD
        );
    }

    /***
     * Parse the output format from the given JSON document map
     *
     * @param document      JSON document map, positioned at top-level
     * @return              OutputFormat, or an exception about a bad type
     */
    private static OutputFormat getOutputFormat(Map<String, Object> document) {

        String fmt = (String) document.get("format");
        if (fmt == null) {
            System.err.println("no format found, defaulting to CSV");
            return OutputFormat.CSV;
        }

        try {
            return Enum.valueOf(OutputFormat.class, fmt);
        } catch (IllegalArgumentException ex) {
            throw new IllegalArgumentException(String.format("unrecognized output format \"%s\"", fmt));
        }
    }

    private static String getOutputFileName(final Map<String, Object> document) throws IOException {
        String filename = (String) document.get("output_filename");
        if (filename == null) {
            System.err.println("no output_filename provided");
            throw new IllegalArgumentException("no output_filename provided");
        }

        if (filename.startsWith("~" + File.separator)) {
            filename = filename.replaceFirst("^~", System.getProperty("user.home"));
        } else if (!filename.startsWith(File.separator)) {
            final String dataPrefixPath = System.getProperty("data.prefix.path");
            if (dataPrefixPath != null) {
                filename = dataPrefixPath + File.separator + filename;
            }
        }
        return filename;
    }

    /***
     * get the output_file name attribute and open a FileWriter for it
     *
     * @param document      JSON document map, positioned at top-level
     * @return              FileWriter object
     */
    private static FileWriter getOutputFile(final Map<String, Object> document) throws IOException {
        final String filename = getOutputFileName(document);
        FileWriter w = new FileWriter(filename);
        return w;
    }

    /***
     * Generates a Parquet-format file given the list of generators. This function exhausts
     * the generators and then closes the file.
     *
     * @param generators    Map of generators, one for each column we expect to write.
     * @throws IOException
     */
    private static void generateParquet(
            final Map<String, Object> document,
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

        CustomParquetWriter pqw2 = getParquetWriter(getOutputFileName(document), mt);

        for (boolean more = true; more; /* inside */ ) {

            List<Object> myRow2 = new LinkedList<>();

            for (Map.Entry<String, DataGenerator> entry : generators.entrySet()) {

                String fieldName = entry.getKey();
                DataGenerator gen = entry.getValue();

                if (!gen.getObjectIterator().hasNext()) {
                    more = false;
                    break;
                }

                Object obj = gen.getObjectIterator().next();
                myRow2.add(obj);
            }

            if (more) {
                pqw2.write(myRow2);
            }
        }

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

            StringBuilder rowBuilder = new StringBuilder();
            first = true;
            for (Map.Entry<String, DataGenerator> entry : generators.entrySet()) {

                String fieldName = entry.getKey();
                DataGenerator gen = entry.getValue();

                if (!gen.getStringIterator().hasNext()) {

                    more = false;
                    break;
                }

                String str = gen.getStringIterator().next();
                if (!first) {
                    rowBuilder.append(',');
                } else {
                    first = false;
                }

                if (str != null)
                    rowBuilder.append(str);
            }

            if (more) {
                outputFile.write(rowBuilder.toString());
                outputFile.append('\n');
            }
        }
    }


    /**
     * generate test data by reading the given JSON file and following the directives within
     *
     * @param filename          String with the filename to be read
     * @throws IOException
     * @throws ParseException
     */
    public static void generateData(final String filename) throws IOException, ParseException {

        // get the JSON file root object as a map of column names to JSON objects
        final JSONObject jsonMap = (JSONObject) new JSONParser().parse(new FileReader(filename));
        final Map<String, Object> documentDictionary = (Map<String, Object>) jsonMap;
        final Map<String, Object> columnDictionary = (Map<String, Object>) documentDictionary.get("columns");

        final OutputFormat format = getOutputFormat(documentDictionary);

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
            generateParquet(documentDictionary, generators);
        } else if (format == OutputFormat.CSV) {

            final FileWriter outputFile;
            try {
                outputFile = getOutputFile(documentDictionary);
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
