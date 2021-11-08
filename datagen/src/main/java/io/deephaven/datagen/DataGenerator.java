package io.deephaven.datagen;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.json.simple.JSONObject;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Locale;

/**
 * DataGenerator implements a partial interface for data generator objects. These objects
 * either produce a "driving" stream of data that is of a known length, or produce a new
 * value each time they're called, without limit.  Each DataGenerator represents a field
 * within the generated data -- 10 columns of output use 10 DataGenerator objects, one to
 * produce each column's value for a given row.
 *
 * Once initialized, a DataGenerator object can produce an Iterator object that allows a
 * client to produce its content.
 */
public abstract class DataGenerator {

    /**
     * Type of the data this column generates; only truly meaningful for Parquet output.
     */
    enum ColumnType {
        DOUBLE,
        STRING,
        INT32,
        INT64,
        TIMESTAMP_NANOS,
    };

    public static final class UnixTimestampNanos {
        final long nanos;
        public UnixTimestampNanos(final long nanos) {
            this.nanos = nanos;
        }
        @Override public String toString() {
            return DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(Instant.ofEpochSecond(0, nanos));
        }
    }

    protected ColumnType columnType;

    /**
     * Creates a DataGenerator object from a JSON representation.
     *
     * @param fieldName     String naming of this field
     * @param jo            JSONObject containing the JSON representation we'll consume
     * @return              an initialized DataGenerator; null if something got sick
     */
    static DataGenerator fromJson(final String fieldName, final JSONObject jo) {

        if (jo == null) {
            throw new IllegalArgumentException("need a JSONObject");
        }

        final String generation_type = Utils.getStringElementValue("generation_type", jo);

        switch (generation_type.toLowerCase(Locale.ROOT)) {
            case "full_range":
                return FullRangeGenerator.fromJson(fieldName, jo);

            case "selection":
                return SelectionGenerator.fromJson(fieldName, jo);

            case "random":
                return RandomGenerator.fromJson(fieldName, jo);

            case "file":
                return ExplicitListGenerator.fromJsonFileGenerator(fieldName, jo);

            case "list":
                return ExplicitListGenerator.fromJsonListGenerator(fieldName, jo);

            case "id":
                return IDGenerator.fromJson(fieldName, jo);

            default:
                throw new IllegalArgumentException(String.format("%s: Unexpected generation_type of %s", fieldName, generation_type));
        }
    }

    /**
     * Helper to get the column type from JSON
     *
     * @param jo        JSONObject of the definition we're reading
     * @return          a ColumnType enum from the JSON representation
     */
    static protected ColumnType columnTypeFromJson(final JSONObject jo) {
        final String type = (String) jo.get("type");
        final ColumnType columnType;
        if (type == null) {
            System.err.printf("No column type specified; defaulting to string\n");
            columnType = ColumnType.STRING;
        } else {
            columnType = Enum.valueOf(ColumnType.class, type);
        }

        return columnType;
    }

    /**
     * Helper to convert from one of our types to a Parquet Type.
     *
     * @param columnType    ColumnType enum for this field
     * @param columnName    String with the name of this field; just for error messages
     * @return              Parquet Type corresponding to the provided ColumnType
     */
    static public Type parquetTypeFromJSONType(ColumnType columnType, String columnName) {
        switch (columnType) {
            case DOUBLE:
                return Types.optional(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnName);
            case INT32:
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT32).named(columnName);
            case INT64:
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT64).named(columnName);
            case STRING:
                return Types.optional(PrimitiveType.PrimitiveTypeName.BINARY)
                        .as(LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType())
                        .named(columnName);
            case TIMESTAMP_NANOS:
                return Types.optional(PrimitiveType.PrimitiveTypeName.INT64)
                        .as(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.timestampType(
                                true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named(columnName);
            default:
                throw new IllegalArgumentException("need to add support for ColumnType." + columnType);
        }
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    /**
     * Gets an Iterator for this generator that produces objects.
     *
     * @return  An intialized Iterator, ready to go.
     */
    public abstract Iterator<Object> getIterator();
}

