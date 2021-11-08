package io.deephaven.datagen;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.List;

public class CustomParquetWriter extends ParquetWriter<Object[]> {
    public final Path path;

    public CustomParquetWriter(
            Path path,
            CustomWriterSupport customWriterSupport,
            boolean enableDictionary,
            CompressionCodecName codecName
    ) throws IOException {
        super(path, customWriterSupport, codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, true);
        this.path = path;
    }
}

