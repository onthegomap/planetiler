package com.onthegomap.planetiler.reader.parquet;

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

public class ParquetInputFile {
  private final ParquetMetadata metadata;
  private final InputFile inputFile;
  private final Path path;

  public ParquetInputFile(Path path) {
    this.path = path;
    inputFile = new InputFile() {
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
    try (var file = ParquetFileReader.open(inputFile)) {
      metadata = file.getFooter();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public long getBlockCount() {
    return metadata.getBlocks().size();
  }

  public interface BlockReader extends Iterable<Block>, Closeable {}

  public interface Block extends Iterable<GenericRecord> {

    Path getFileName();
  }

  public BlockReader get() {
    var config = new Configuration();
    var schema = metadata.getFileMetaData().getSchema();
    var columnIOFactory = new ColumnIOFactory(metadata.getFileMetaData().getCreatedBy(), false);
    var keyValueMetadata = metadata.getFileMetaData().getKeyValueMetaData();
    var keyValueMetadataSets = keyValueMetadata.entrySet().stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> Set.of(e.getValue())));
    try {
      ParquetFileReader reader = ParquetFileReader.open(inputFile);
      return new BlockReader() {
        @Override
        public void close() throws IOException {
          reader.close();
        }

        @Override
        public Iterator<Block> iterator() {
          return metadata.getBlocks().stream().map(block -> {
            try {
              // happens in reader thread
              var group = reader.readRowGroup(block.getOrdinal());
              return (Block) new Block() {
                @Override
                public Path getFileName() {
                  return path;
                }

                @Override
                public Iterator<GenericRecord> iterator() {
                  // happens in worker thread
                  AvroReadSupport<GenericRecord> readSupport = new AvroReadSupport<>(GenericData.get());
                  var readContext = readSupport.init(new InitContext(config, keyValueMetadataSets, schema));
                  MessageColumnIO columnIO = columnIOFactory.getColumnIO(schema);
                  var converter = readSupport.prepareForRead(config, keyValueMetadata, schema, readContext);
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
                }
              };
            } catch (IOException e) {
              throw new UncheckedIOException(e);
            }
          }).iterator();
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public long getCount() {
    return metadata.getBlocks().stream().mapToLong(BlockMetaData::getRowCount).sum();
  }

}
