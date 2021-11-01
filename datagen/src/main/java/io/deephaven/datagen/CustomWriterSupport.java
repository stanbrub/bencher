package io.deephaven.datagen;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.ParquetEncodingException;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.HashMap;
import java.util.List;

public class CustomWriterSupport extends WriteSupport<List<Object>> {
    MessageType schema;
    RecordConsumer recordConsumer;
    List<ColumnDescriptor> cols;

    // TODO: support specifying encodings and compression
    CustomWriterSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    public void write(List<Object> values) {
        if (values.size() != cols.size()) {
            throw new ParquetEncodingException("Invalid input data. Expecting " +
                    cols.size() + " columns. Input had " + values.size() + " columns (" + cols + ") : " + values);
        }

        recordConsumer.startMessage();
        for (int i = 0; i < cols.size(); ++i) {

            final Object val = values.get(i);
            if (val == null) {

            } else {
                recordConsumer.startField(cols.get(i).getPath()[0], i);
                PrimitiveType.PrimitiveTypeName ptn = cols.get(i).getPrimitiveType().getPrimitiveTypeName();
                switch (ptn) {
                    case BOOLEAN:
                        recordConsumer.addBoolean((Boolean) val);
                        break;
                    case DOUBLE:
                        recordConsumer.addDouble((double) val);
                        break;
                    case INT32:
                        recordConsumer.addInteger((Integer) val);
                        break;
                    case INT64:
                        recordConsumer.addLong((Long) val);
                        break;
                    case BINARY:
                        recordConsumer.addBinary(stringToBinary((String) val));
                        break;
                    default:
                        throw new ParquetEncodingException(
                                "Unsupported column type: " + ptn);
                }
                recordConsumer.endField(cols.get(i).getPath()[0], i);
            }
        }
        recordConsumer.endMessage();
    }


    private Binary stringToBinary(String value) {
        return Binary.fromString(value.toString());
    }
}

