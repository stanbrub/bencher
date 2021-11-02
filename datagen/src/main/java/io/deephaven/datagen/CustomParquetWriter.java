package io.deephaven.datagen;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public class CustomParquetWriter extends ParquetWriter<Object[]> {

    public CustomParquetWriter(
            Path file,
            CustomWriterSupport customWriterSupport,
            boolean enableDictionary,
            CompressionCodecName codecName
    ) throws IOException {
        super(file, customWriterSupport, codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, true);
    }
}

