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
    private final MessageType schema;
    private RecordConsumer recordConsumer;
    private final List<ColumnDescriptor> cols;
    private final int ncols;

    CustomWriterSupport(final MessageType schema) {
        this.schema = schema;
        this.cols = schema.getColumns();
        this.ncols = cols.size();
    }

    @Override
    public WriteContext init(final Configuration config) {
        return new WriteContext(schema, new HashMap<>());
    }

    @Override
    public void prepareForWrite(final RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    public void write(final Object[] buffer) {
        recordConsumer.startMessage();
        for (int i = 0; i < ncols; ++i) {
            final String field = cols.get(i).getPath()[0];
            final Object val = buffer[i];
            if (val != null) {
                recordConsumer.startField(field, i);
                final PrimitiveType.PrimitiveTypeName ptn = cols.get(i).getPrimitiveType().getPrimitiveTypeName();
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
                        final long v;
                        if (val instanceof DataGenerator.UnixTimestampNanos) {
                            v = ((DataGenerator.UnixTimestampNanos) val).nanos;
                        } else if (val instanceof Long) {
                            v = (Long) val;
                        } else {
                            throw new IllegalStateException(
                                    "Unknown object type: " + val.getClass().getCanonicalName() + " with value " + val);
                        }
                        recordConsumer.addLong(v);
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

    private Binary stringToBinary(final String value) {
        return Binary.fromString(value);
    }
}

