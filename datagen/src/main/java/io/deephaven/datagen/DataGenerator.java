package io.deephaven.datagen;

import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.json.simple.JSONObject;

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
    };

    protected ColumnType columnType;

    abstract int getCapacity();

    /**
     * Creates a DataGenerator object from a JSON representation.
     *
     * @param fieldName     String naming of this field
     * @param jo            JSONObject containing the JSON representation we'll consume
     * @return              an initialized DataGenerator; null if something got sick
     */
    static DataGenerator fromJson(String fieldName, JSONObject jo) {

        if (jo == null) {
            throw new IllegalArgumentException("need a JSONObject");
        }

        String generation_type = (String) jo.get("generation_type");
        if (generation_type == null) {
            throw new IllegalArgumentException(String.format("%s: generation_type must be supplied", fieldName));
        }

        DataGenerator gen = null;

        switch (generation_type.toLowerCase(Locale.ROOT)) {
            case "full_range":
                gen = FullRangeGenerator.fromJson(fieldName, jo);
                break;

            case "selection":
                gen = SelectionGenerator.fromJson(fieldName, jo);
                break;

            case "random":
                gen = RandomGenerator.fromJson(fieldName, jo);
                break;

            case "file":
                gen = FileGenerator.fromJson(fieldName, jo);
                break;

            case "id":
                gen = IDGenerator.fromJson(fieldName, jo);
                break;

            default:
                throw new IllegalArgumentException(String.format("%s: Unexpected generation_type of %s", fieldName, generation_type));
        }

        return gen;
    }

    /**
     * Helper to get the column type from JSON
     *
     * @param jo        JSONObject of the definition we're reading
     * @return          a ColumnType enum from the JSON representation
     */
    static protected ColumnType columnTypeFromJson(JSONObject jo) {

        String type = (String) jo.get("type");
        ColumnType columnType = ColumnType.STRING;
        if (type == null) {
            System.err.printf("No column type specified; defaulting to string\n");
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
                return Types.required(PrimitiveType.PrimitiveTypeName.DOUBLE).named(columnName);
            case INT32:
                return Types.required(PrimitiveType.PrimitiveTypeName.INT32).named(columnName);
            case INT64:
                return Types.required(PrimitiveType.PrimitiveTypeName.INT64).named(columnName);
            case STRING:
                return Types.required(PrimitiveType.PrimitiveTypeName.BINARY).as(LogicalTypeAnnotation.StringLogicalTypeAnnotation.stringType()).named(columnName);
            default:
                throw new IllegalArgumentException("need to add support for ColumnType." + columnType);
        }
    }

    public ColumnType getColumnType() {
        return columnType;
    }

    /**
     * Gets an Iterator for this generator that produces strings.
     *
     * @return  An intialized Iterator, ready to go.
     */
    public abstract Iterator<String> getStringIterator();

    /**
     * Gets an Iterator for this generator that produces objects.
     *
     * @return  An intialized Iterator, ready to go.
     */
    public abstract Iterator<Object> getObjectIterator();
}

