package io.deephaven.datagen;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

// https://github.com/fangyidong/json-simple
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Types;

public class DataGen {

    private static final boolean OVERWRITE = Boolean.parseBoolean(System.getProperty(
            "data.overwrite", "true"));
    private static final boolean FORCE_GENERATION = Boolean.parseBoolean(System.getProperty(
            "force.generation", "False"));

    private enum OutputFormat {
        PARQUET,
        CSV,
    }

    /***
     * Construct a {@code ParquetWriter} given the MessageType schema that we're passed.
     *
     * @param outputFilePath         Where the target file will live.
     * @param customWriterSupport    {@code CustomWriterSupport} for the schema we will be writing.
     * @return                       {@code ParquetWriter} ready to write
     * @throws IOException
     */
    private static ParquetWriter<Object[]> getParquetWriter(final String outputFilePath, CustomWriterSupport customWriterSupport) throws IOException {

        final File outputParquetFile = new File(outputFilePath);
        if (outputParquetFile.exists() && OVERWRITE) {
            outputParquetFile.delete();
        }
        final Path path = new Path(outputParquetFile.toURI().toString());
        ParquetWriter.Builder<Object[], ?> parquetWriterBuilder = new ParquetWriter.Builder(path) {
            @Override
            protected ParquetWriter.Builder self() {
                return this;
            }

            @Override
            protected WriteSupport<Object[]> getWriteSupport(org.apache.hadoop.conf.Configuration conf) {
                return customWriterSupport;
            }
        };
        parquetWriterBuilder.withCompressionCodec(CompressionCodecName.GZIP);

        return parquetWriterBuilder.build();
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

    private static String getOutputFilename(
            final String outputPrefixPath, final String generatorFilename, final OutputFormat format) {
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
        return outputPrefixPath + File.separator + basename + "." + extension;
    }

    /***
     * Generates a Parquet-format file given the list of generators. This function exhausts
     * the generators and then closes the file.
     *
     * @param outputFileName  Filename to write output to.
     * @param columns         Column names for the columns we expect to write.
     * @param generators      Array of generators, one for each column we expect to write.
     * @throws IOException
     */
    private static void generateParquet(
            final String outputFileName,
            final String[] columns,
            final DataGenerator[] generators) throws IOException {

        // build typed Parquet structure
        // need a MessageTypeBuilder so we can create the protobuf type that Parquet uses
        Types.MessageTypeBuilder builder = org.apache.parquet.schema.Types.buildMessage();
        for (int i = 0; i < columns.length; ++i) {
            builder.addField(DataGenerator.parquetTypeFromJSONType(generators[i].getColumnType(), columns[i]));
        }

        MessageType mt = builder.named("MyMessage");

        final CustomWriterSupport customWriterSupport = new CustomWriterSupport(mt);
        ParquetWriter<Object[]> pqw2 = getParquetWriter(outputFileName, customWriterSupport);

        final Object[] data = new Object[columns.length];
        boolean more = true;
        int row = 0;
        try {
            while (more) {
                ++row;
                for (int i = 0; i < columns.length; ++i) {
                    final DataGenerator dg = generators[i];
                    final Iterator<Object> iter = dg.getIterator();
                    if (!iter.hasNext()) {
                        more = false;
                        break;
                    }
                    data[i] = iter.next();
                }
                if (more) {
                    pqw2.write(data);
                }
            }
        } catch (Exception ex) {
            new File(outputFileName).delete();
            throw new RuntimeException(String.format("Failure while generating at row %d", row), ex);
        }

        customWriterSupport.flush();
        pqw2.close();
    }

    /***
     * Generates a CSV file from the list of generators. This function will exhaust the
     * generators. Also writes a header at the first row, using the column names.
     *
     * @param outputFile    A {@FileWriter} where we will write to.
     * @param columns       Column names for the columns we expect to write.
     * @param generators    Array of generators, one for each column we expect to write.
     */
    private static void generateCSV(
            final FileWriter outputFile,
            final String[] columns,
            final DataGenerator[] generators
    ) throws IOException {

        StringBuilder headerBuilder = new StringBuilder();
        for (int i = 0; i < columns.length; ++i) {
            if (i != 0) {
                headerBuilder.append(',');
            }

            headerBuilder.append(columns[i]);
        }
        outputFile.write(headerBuilder.toString());
        outputFile.append('\n');

        for (boolean more = true; more; /* inside */ ) {
            final StringBuilder rowBuilder = new StringBuilder();
            for (int i = 0; i < columns.length; ++i) {
                final DataGenerator gen = generators[i];

                if (!gen.getIterator().hasNext()) {
                    more = false;
                    break;
                }

                final Object val = gen.getIterator().next();
                if (i != 0) {
                    rowBuilder.append(',');
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
     * Generate test data by reading the given JSON file and following the directives within
     *
     * @oaram outputPrefixPath           Base directory for generated data.
     * @param dir                        A directory relative to which interpret the generatorFilename.
     * @param generatorFilename          String with the generatorFilename to be read
     * @throws IOException
     * @throws ParseException
     */
    public static void generateData(
            final String outputPrefixPath,
            final File dir,
            final String generatorFilename
    ) throws IOException, ParseException {
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
        final String outputFilename = getOutputFilename(outputPrefixPath, generatorFilename, format);

        if (!FORCE_GENERATION) {
            final File outputFile = new File(outputFilename);
            if (outputFile.exists() && outputFile.lastModified() > generatorFile.lastModified()) {
                System.out.println("Not generating " + outputFile.getAbsolutePath() +
                        " since it exists and is older than " + generatorFile.getAbsolutePath());
                return;
            }
        }
        System.out.println("Generating " + outputFilename + ".");

        final Object columnsObject = documentDictionary.get("columns");
        final Map<String, Object> columnDictionary;
        final String[] columns;
        if (columnsObject instanceof Map) {
            columnDictionary = (Map<String, Object>) columnsObject;
            columns = columnDictionary.keySet().toArray(new String[columnDictionary.size()]);
        } else if (columnsObject instanceof List) {
            final List<Object> columnList = (List) columnsObject;
            columnDictionary = new HashMap<>();
            columns = new String[columnList.size()];
            int i = 0;
            for (Object element : columnList) {
                final JSONObject jo = (JSONObject) element;
                final String column = Utils.getStringElementValue("name", jo);
                columns[i++] = column;
                columnDictionary.put(column, jo);
            }
        } else {
            throw new IllegalArgumentException(
                    "element \"columns\" has the wrong type: " + columnsObject.getClass().getSimpleName());
        }

        // map from string (name of column) to our io.deephaven.datagen.DataGenerator-derived objects
        final DataGenerator[] generators = new DataGenerator[columns.length];

        // for each entry in the JSON document ...
        int i = 0;
        for (final String column : columns) {

            // get the column name and the JSON object
            final JSONObject jsonField = (JSONObject) columnDictionary.get(column);

            // create that object and dump it into the map
            final DataGenerator gen = DataGenerator.fromJson(column, jsonField);
            generators[i++] = gen;
        }

        if (format == OutputFormat.PARQUET) {
            generateParquet(outputFilename, columns, generators);
        } else if (format == OutputFormat.CSV) {

            final FileWriter outputFile;
            try {
                outputFile = new FileWriter(outputFilename);
            } catch (IOException ex) {
                String err = String.format("Couldn't create output file: %s\n", ex);
                System.err.printf(err);
                throw new InternalError(err);
            }

            generateCSV(outputFile, columns, generators);
            outputFile.close();
        } else {
            throw new InternalError(String.format("Not ready to handle format %s", format));
        }
    }

}
