package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.util.FileUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReadStore;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReadStoreImpl;
import org.apache.parquet.example.DummyRecordConverter;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.SeekableInputStream;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;

public class ParquetInputFile implements Closeable {

  private final ParquetFileReader reader;
  private final ParquetMetadata metadata;

  public ParquetInputFile(Path path) {
    var inputFile = new InputFile() {
      @Override
      public long getLength() {
        return FileUtils.size(path);
      }

      @Override
      public SeekableInputStream newStream() throws IOException {
        var channel = Files.newByteChannel(path);
        var inputStream = Channels.newInputStream(channel);

        return new DelegatingSeekableInputStream(inputStream) {
          private long position;

          @Override
          public long getPos() {
            return position;
          }

          @Override
          public void seek(long newPos) throws IOException {
            channel.position(newPos);
            position = newPos;
          }
        };
      }
    };
    try {
      this.reader = ParquetFileReader.open(inputFile);
      this.metadata = reader.getFooter();
      for (var block : metadata.getBlocks()) {
        var group = reader.readRowGroup(block.getOrdinal());
        ColumnReadStore columnReadStore =
          new ColumnReadStoreImpl(group,
            new DummyRecordConverter(metadata.getFileMetaData().getSchema()).getRootConverter(),
            metadata.getFileMetaData()
              .getSchema(),
            metadata.getFileMetaData().getCreatedBy());
        for (var column : metadata.getFileMetaData().getSchema().getColumns()) {
          var reader = columnReadStore.getColumnReader(column);
          //          reader.get
        }
      }
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public interface BlockReader extends Iterable<Block> {}

  public interface Block extends Iterable<ParquetRow> {}

  public static class ParquetRow {

    private final Map<String, Object> map;

    private ParquetRow(Map<String, Object> map) {
      this.map = map;
    }

    public Object get(String name) {
      return map.get(name);
    }

    @Override
    public String toString() {
      return map.toString();
    }
  }

  public BlockReader get() {
    var schema = metadata.getFileMetaData().getSchema();
    var columns = schema.getColumns();
    System.err.println(metadata.getBlocks().size());
    return () -> metadata.getBlocks().stream().map(block -> {
      try {
        // happens in reader thread
        var group = reader.readRowGroup(block.getOrdinal());
        return (Block) (() -> {
          // happens in each worker thread
          ColumnReadStore columnReadStore =
            new ColumnReadStoreImpl(group, new GroupRecordConverter(schema).getRootConverter(),
              schema, metadata.getFileMetaData().getCreatedBy());
          var currentRowGroupColumnReaders = columns.stream().map(columnReadStore::getColumnReader).toList();
          return new Iterator<>() {
            long index = 0;
            long[] read = new long[columns.size()];

            @Override
            public boolean hasNext() {
              boolean result = index < group.getRowCount();
              if (!result) {
                boolean failed = false;
                for (int i = 0; i < read.length; i++) {
                  var columnReader = currentRowGroupColumnReaders.get(i);
                  if (read[i] != columnReader.getTotalValueCount()) {
                    System.err.println(String.join(".", columns.get(i).getPath()) + " expected " +
                      columnReader.getTotalValueCount() + " but read " + read[i]);
                    failed = true;
                  }
                }
                if (failed) {
                  throw new IllegalStateException();
                }
              }
              return result;
            }

            @Override
            public ParquetRow next() {
              Map<String, Object> result = new HashMap<>();
              int i = 0;
              for (ColumnReader columnReader : currentRowGroupColumnReaders) {
                String path = String.join(".", columnReader.getDescriptor().getPath());
                result.put(path, readValue(columnReader));
                columnReader.consume();
                read[i++]++;

                // how many optional fields in the path for the column are defined
                int def = columnReader.getCurrentDefinitionLevel();
                // at what repeated field in the path has the value repeated
                int rep = columnReader.getCurrentRepetitionLevel();
              }
              index++;
              return new ParquetRow(result);
            }
          };
        });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).iterator();


    //      () -> {
    //      try {
    //        for (var block : metadata.getBlocks()) {
    //          var group = reader.readRowGroup(block.getOrdinal());
    //          consumer.accept(() -> () -> {
    //            ColumnReadStore columnReadStore =
    //              new ColumnReadStoreImpl(group, new DummyRecordConverter(schema).getRootConverter(),
    //                metadata.getFileMetaData().getSchema(), metadata.getFileMetaData().getCreatedBy());
    //            return () -> {
    //              for (var column : schema.getColumns()) {
    //                var reader = columnReadStore.getColumnReader(column);
    //                //          reader.get
    //              }
    //            };
    //          });
    //        }
    //      } catch (IOException e) {
    //        throw new UncheckedIOException(e);
    //      }
    //    };
  }

  public static void main(String[] args) throws IOException {
    var path = Path.of(
      "./data/sources/overture/theme=buildings/type=building/20230725_211555_00082_tpd52_00e93efa-24f4-4014-b65a-38c7d2854ab4");
    try (var parquetInputFile = new ParquetInputFile(path)) {
      for (var block : parquetInputFile.get()) {
        for (var row : block) {
          //          System.out.println(row);
        }
      }
    }
  }

  private static Object readValue(ColumnReader columnReader) {
    ColumnDescriptor column = columnReader.getDescriptor();
    PrimitiveType primitiveType = column.getPrimitiveType();
    int maxDefinitionLevel = column.getMaxDefinitionLevel();

    if (columnReader.getCurrentDefinitionLevel() == maxDefinitionLevel) {
      return switch (primitiveType.getPrimitiveTypeName()) {
        case BINARY, FIXED_LEN_BYTE_ARRAY, INT96 -> primitiveType.getLogicalTypeAnnotation() == LogicalTypeAnnotation
          .stringType() ?
            columnReader.getBinary().toStringUsingUTF8() :
            columnReader.getBinary().getBytesUnsafe();
        case BOOLEAN -> columnReader.getBoolean();
        case DOUBLE -> columnReader.getDouble();
        case FLOAT -> columnReader.getFloat();
        case INT32 -> columnReader.getInteger();
        case INT64 -> columnReader.getLong();
      };
    } else {
      return null;
    }
  }

  public long getCount() {
    return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
