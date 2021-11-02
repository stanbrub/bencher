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

public class CustomWriterSupport extends WriteSupport<Object[]> {
    final MessageType schema;
    RecordConsumer recordConsumer;
    final List<ColumnDescriptor> cols;
    final int ncols;

    CustomWriterSupport(MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
        this.ncols = cols.size();
    }

    @Override
    public WriteContext init(Configuration config) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    public void write(Object[] buffer) {
        recordConsumer.startMessage();
        for (int i = 0; i < ncols; ++i) {
            final String field = cols.get(i).getPath()[0];
            final Object val = buffer[i];
            if (val != null) {
                recordConsumer.startField(field, i);
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
                recordConsumer.endField(field, i);
            }
        }
        recordConsumer.endMessage();
    }

    public void flush() {
        recordConsumer.flush();
    }

    private Binary stringToBinary(String value) {
        return Binary.fromString(value);
    }
}

