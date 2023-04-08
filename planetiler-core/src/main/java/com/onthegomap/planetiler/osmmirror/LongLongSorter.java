package com.onthegomap.planetiler.osmmirror;

import static com.onthegomap.planetiler.util.MemoryEstimator.CLASS_HEADER_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.LONG_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.POINTER_BYTES;

import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.atomic.AtomicLong;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface LongLongSorter extends Iterable<LongLongSorter.Result>, DiskBacked {
  Logger LOGGER = LoggerFactory.getLogger(LongLongSorter.class);

  void put(long a, long b);

  long count();

  record Result(long a, long b) implements Comparable<Result> {

    @Override
    public int compareTo(Result o) {
      int comp = Long.compare(a, o.a);
      if (comp == 0) {
        comp = Long.compare(b, o.b);
      }
      return comp;
    }
  }

  class DiskBacked implements LongLongSorter {

    private static final long BYTES_PER_ENTRY = CLASS_HEADER_BYTES + LONG_BYTES * 2 + POINTER_BYTES;
    private final long limit;
    private final Stats stats;
    private final int maxWorkers;
    long count = 0;
    boolean prepared;
    List<Chunk> chunks = new ArrayList<>();
    Chunk chunk;

    @Override
    public long count() {
      return chunks.stream().mapToLong(d -> d.count).sum();
    }

    @Override
    public long diskUsageBytes() {
      return chunks.stream().mapToLong(c -> FileUtils.size(c.path)).sum();
    }

    class Chunk implements Iterable<Result> {

      private final MessagePacker packer;

      Chunk() {
        path = tmpDir.resolve("chunk-" + chunks.size());
        chunks.add(this);
        try {
          packer =
            MessagePack.newDefaultPacker(FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.WRITE));
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      Path path;
      int count = 0;

      public void sort() {
        Result[] array = new Result[count];
        int i = 0;
        try (var iter = iterator()) {
          while (iter.hasNext()) {
            array[i++] = iter.next();
          }
        }
        Arrays.parallelSort(array);
        try (
          var p = MessagePack
            .newDefaultPacker(FileChannel.open(path, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE))
        ) {
          for (var item : array) {
            p.packLong(item.a).packLong(item.b);
          }
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public CloseableIterator<Result> iterator() {
        try {
          var unpacker = MessagePack.newDefaultUnpacker(FileChannel.open(path, StandardOpenOption.READ));
          return new CloseableIterator<>() {
            int i = 0;

            @Override
            public void close() {
              try {
                unpacker.close();
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }

            @Override
            public boolean hasNext() {
              return i < count;
            }

            @Override
            public Result next() {
              try {
                i++;
                return new Result(unpacker.unpackLong(), unpacker.unpackLong());
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            }
          };
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }

    private final Path tmpDir;

    public DiskBacked(Path nodeToWayTmp, Stats stats, long limit, int maxWorkers) {
      tmpDir = nodeToWayTmp;
      FileUtils.createDirectory(nodeToWayTmp);
      chunk = new Chunk();
      this.stats = stats;
      this.limit = limit;
      this.maxWorkers = maxWorkers;
    }

    public DiskBacked(Path nodeToWayTmp, Stats stats, int maxWorkers) {
      this(nodeToWayTmp, stats, Math.max(1_000_000_000, ProcessInfo.getMaxMemoryBytes() / 8) / BYTES_PER_ENTRY,
        maxWorkers);
    }

    private void prepare() {
      if (!prepared) {
        var timer = Timer.start();
        try {
          chunk.packer.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        LOGGER.info("Sorting {} way members...", count());
        int sortThreads = Math.min(maxWorkers,
          Math.max(1, (int) (ProcessInfo.getMaxMemoryBytes() / 2 / BYTES_PER_ENTRY / limit)));
        AtomicLong done = new AtomicLong(0);
        var pipeline = WorkerPipeline.start("sort", stats)
          .readFromTiny("chunks", chunks)
          .sinkToConsumer("sort", sortThreads, (chunk) -> {
            chunk.sort();
            done.incrementAndGet();
          });
        ProgressLoggers logger = ProgressLoggers.create()
          .addPercentCounter("sort", chunks.size(), done)
          .addFileSize(this::diskUsageBytes)
          .newLine()
          .addPipelineStats(pipeline)
          .newLine()
          .addProcessStats();
        pipeline.awaitAndLog(logger, Duration.ofSeconds(10));
        LOGGER.info("Sorted way members in {}", timer.stop());
        prepared = true;
      }
    }

    class PeekableIterator implements Comparable<PeekableIterator>, CloseableIterator<Result> {

      private final CloseableIterator<Result> iterator;
      Result next;

      public PeekableIterator(CloseableIterator<Result> iterator) {
        this.iterator = iterator;
        next = iterator.next();
      }

      @Override
      public int compareTo(PeekableIterator o) {
        return next.compareTo(o.next);
      }

      @Override
      public void close() {
        iterator.close();
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      @Override
      public Result next() {
        var result = next;
        next = iterator.hasNext() ? iterator.next() : null;
        return result;
      }
    }

    @Override
    public CloseableIterator<Result> iterator() {
      prepare();
      PriorityQueue<PeekableIterator> pq = new PriorityQueue<>();
      for (var c : chunks) {
        if (c.count > 0) {
          pq.offer(new PeekableIterator(c.iterator()));
        }
      }
      return new CloseableIterator<>() {
        @Override
        public void close() {
          pq.forEach(PeekableIterator::close);
        }

        @Override
        public boolean hasNext() {
          return !pq.isEmpty();
        }

        @Override
        public Result next() {
          var item = pq.poll();
          var result = item.next();
          if (item.hasNext()) {
            pq.offer(item);
          } else {
            item.close();
          }
          return result;
        }
      };
    }

    public void put(long a, long b) {
      if (prepared) {
        throw new IllegalStateException("Already prepared");
      }
      try {
        if (chunk.count > limit) {
          chunk.packer.close();
          chunk = new Chunk();
        }
        chunk.packer.packLong(a).packLong(b);
        chunk.count++;
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  class InMemory implements LongLongSorter {

    private List<Result> items = new ArrayList<>();

    @Override
    public Iterator<Result> iterator() {
      items.sort(Comparator.naturalOrder());
      return items.iterator();
    }

    public void put(long a, long b) {
      items.add(new Result(a, b));
    }

    @Override
    public long count() {
      return items.size();
    }

    @Override
    public long diskUsageBytes() {
      return 0;
    }
  }
}
