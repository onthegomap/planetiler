package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.util.FileUtils;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.Channels;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.api.InitContext;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.DelegatingSeekableInputStream;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.io.MessageColumnIO;
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
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public interface BlockReader extends Iterable<Block> {}

  public interface Block extends Iterable<GenericRecord> {}

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
    var config = new Configuration();
    var schema = metadata.getFileMetaData().getSchema();
    ColumnIOFactory columnIOFactory = new ColumnIOFactory(metadata.getFileMetaData().getCreatedBy(), false);
    var keyValueMetadata = metadata.getFileMetaData().getKeyValueMetaData();
    var keyValueMetadataSets =
      keyValueMetadata.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Set.of(e.getValue())));
    return () -> metadata.getBlocks().stream().map(block -> {
      try {
        // happens in reader thread
        var group = reader.readRowGroup(block.getOrdinal());
        return (Block) (() -> {
          AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>(GenericData.get());
          var readContext = readSupport.init(new InitContext(config, keyValueMetadataSets, schema));
          MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
          var converter =
            readSupport.prepareForRead(config, keyValueMetadata, schema, readContext);
          var recordReader = columnIO.getRecordReader(group, converter, FilterCompat.NOOP);
          long total = block.getRowCount();
          return new Iterator<>() {
            long i = 0;

            @Override
            public boolean hasNext() {
              return i < total;
            }

            @Override
            public GenericRecord next() {
              if (!hasNext()) {
                throw new NoSuchElementException();
              }
              i++;
              return recordReader.read();
            }
          };
        });
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).iterator();
  }

  public static void main(String[] args) throws IOException {
    //    var path = Path.of(
    //      "./data/sources/overture/theme=admins/type=administrativeBoundary/20230725_211237_00132_5p54t_0100b1b9-31ab-4d03-9a92-8141dcae93a5");
    var path = Path.of(
      "./data/sources/overture/theme=buildings/type=building/20230725_211555_00082_tpd52_00e93efa-24f4-4014-b65a-38c7d2854ab4");
    try (var parquetInputFile = new ParquetInputFile(path)) {
      for (var block : parquetInputFile.get()) {
        for (var row : block) {
          if (row.get("height")instanceof Number n) {
            System.err.println(row);
          }
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
