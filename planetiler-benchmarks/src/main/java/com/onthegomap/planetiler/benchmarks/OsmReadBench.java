package com.onthegomap.planetiler.benchmarks;

import static java.nio.file.StandardOpenOption.READ;

import com.google.protobuf.ByteString;
import com.graphhopper.reader.ReaderElement;
import com.onthegomap.planetiler.collection.IterableOnce;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.PbfDecoder;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.Worker;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Inflater;
import org.apache.lucene.store.NativePosixUtil;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

public class OsmReadBench {

  static {
    try {
      Class.forName("org.apache.lucene.store.NativePosixUtil");
      System.out.println("Successfully loaded NativePosixUtil, madvise should work");
    } catch (ClassNotFoundException | UnsatisfiedLinkError | NoClassDefFoundError e) {
      System.err.println("Failed to load NativePosixUtil, madvise random won't do anything:  " + e);
    }
  }

  public static final int cpus = Runtime.getRuntime().availableProcessors();

  static class Counts {

    long nodes = 0;
    long ways = 0;
    long rels = 0;

    @Override
    public String toString() {
      return "Counts{" +
        "nodes=" + nodes +
        ", ways=" + ways +
        ", rels=" + rels +
        '}';
    }

    public static Counts sum(Counts a, Counts b) {
      var c = new Counts();
      c.nodes = a.nodes + b.nodes;
      c.ways = a.ways + b.ways;
      c.rels = a.rels + b.rels;
      return c;
    }
  }

  public static void benchmarkExistingGraphhopper(String file) throws Exception {
    var osmInputFile = new OsmInputFile(Path.of(file));

    var pipeline = WorkerPipeline.start("test", Stats.inMemory())
      .fromGenerator("pbf", osmInputFile.read("parse", cpus - 1))
      .addBuffer("buffer", 50_000, 10_000)
      .sinkTo("count", 1, elements -> {
        ReaderElement elem;
        var counts = new Counts();
        while ((elem = elements.get()) != null) {
          switch (elem.getType()) {
            case ReaderElement.NODE -> counts.nodes++;
            case ReaderElement.WAY -> counts.ways++;
            case ReaderElement.RELATION -> counts.rels++;
          }
        }
      });
    pipeline.awaitAndLog(ProgressLoggers.create()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats(), Duration.ofSeconds(10));
  }

  public static void benchmarkNew(String file) {
    var countList = new CopyOnWriteArrayList<Counts>();
    var path = Path.of(file);
    AtomicLong blocks = new AtomicLong(0);
    AtomicLong nodes = new AtomicLong(0);
    AtomicLong ways = new AtomicLong(0);
    AtomicLong rels = new AtomicLong(0);

    var pipeline = WorkerPipeline.start("countNewReader", Stats.inMemory())
      .<byte[]>fromGenerator("read", (next) -> {
        try (var dataInput = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 50_000))) {
          while (true) {
            int headerSize = dataInput.readInt();
            Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(dataInput.readNBytes(headerSize));
            byte[] buf = dataInput.readNBytes(header.getDatasize());
            if ("OSMData".equals(header.getType())) {
              next.accept(buf);
            }
          }
        } catch (EOFException e) {
          // done
        }
      }, 1)
      .addBuffer("buffer", 100, 1)
      .sinkTo("count", cpus, elements -> {
        final Counts stats = new Counts();
        countList.add(stats);
        byte[] bytes;
        while ((bytes = elements.get()) != null) {
          PbfDecoder.decode(bytes, elem -> {
            if (elem instanceof OsmElement.Node) {
              stats.nodes++;
            } else if (elem instanceof OsmElement.Way) {
              stats.ways++;
            } else if (elem instanceof OsmElement.Relation) {
              stats.rels++;
            }
          });
          blocks.incrementAndGet();
          Counts n = countList.stream().reduce(Counts::sum).get();
          nodes.set(n.nodes);
          ways.set(n.ways);
          rels.set(n.rels);
        }
      });
    pipeline.awaitAndLog(ProgressLoggers.create()
      .addRateCounter("blocks", blocks)
      .addRateCounter("nodes", nodes)
      .addRateCounter("ways", ways)
      .addRateCounter("rels", rels)
      .newLine()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats(), Duration.ofSeconds(10));

    System.err.println(countList.stream().reduce(Counts::sum));
  }

  public static void benchmarkNewIoThreads(String file) {
    var countList = new CopyOnWriteArrayList<Counts>();
    var path = Path.of(file);
    record Segment(long start, int length) {}

    var pipeline = WorkerPipeline.start("countNewReader", Stats.inMemory())
      .<Segment>fromGenerator("read", (next) -> {
        try (FileChannel channel = FileChannel.open(path, READ)) {
          while (channel.position() < channel.size()) {
            ByteBuffer buf = ByteBuffer.allocate(4);
            channel.read(buf);
            buf = ByteBuffer.allocate(buf.flip().getInt());
            channel.read(buf);
            Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(buf.flip());
            long start = channel.position();
            int length = header.getDatasize();
            channel.position(channel.position() + length);
            if ("OSMData".equals(header.getType())) {
              next.accept(new Segment(start, length));
            }
          }
        }
      }, 1)
      .addBuffer("buffer", 100, 1)
      .sinkTo("count", cpus, elements -> {
        final Counts stats = new Counts();
        countList.add(stats);
        Segment seg;
        try (var channel = FileChannel.open(path, READ)) {
          while ((seg = elements.get()) != null) {
            channel.position(seg.start);
            ByteBuffer buf = ByteBuffer.allocate(seg.length);
            channel.read(buf);
            PbfDecoder.decode(buf.flip().array(), elem -> {
              if (elem instanceof OsmElement.Node) {
                stats.nodes++;
              } else if (elem instanceof OsmElement.Way) {
                stats.ways++;
              } else if (elem instanceof OsmElement.Relation) {
                stats.rels++;
              }
            });
          }
        }
      });
    pipeline.awaitAndLog(ProgressLoggers.create()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats(), Duration.ofSeconds(10));

    System.err.println(countList.stream().reduce(Counts::sum));
  }

  public static void benchmarkJustPbf(String file) throws Exception {
    var path = Path.of(file);
    var countList = new CopyOnWriteArrayList<Counts>();

    var pipeline =
      WorkerPipeline.start("test", Stats.inMemory())
        .<byte[]>fromGenerator("reader", (next) -> {
          try (var dataInput = new DataInputStream(new BufferedInputStream(Files.newInputStream(path), 50_000))) {
            while (true) {
              int headerSize = dataInput.readInt();
              byte[] buf = dataInput.readNBytes(headerSize);
              Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(buf);
              buf = dataInput.readNBytes(header.getDatasize());
              if ("OSMData".equals(header.getType())) {
                next.accept(buf);
              }
            }
          } catch (EOFException e) {
            // done
          }
        }).addBuffer("buffer", 100)
        .sinkTo("counter", cpus - 1, prev -> {
          Counts stats = new Counts();
          countList.add(stats);

          byte[] buf;
          while ((buf = prev.get()) != null) {
            Fileformat.Blob blob = Fileformat.Blob.parseFrom(buf);
            ByteString data;
            if (blob.hasRaw()) {
              data = blob.getRaw();
            } else if (blob.hasZlibData()) {
              byte[] buf2 = new byte[blob.getRawSize()];
              Inflater decompresser = new Inflater();
              decompresser.setInput(blob.getZlibData().toByteArray());
              decompresser.inflate(buf2);
              decompresser.end();
              data = ByteString.copyFrom(buf2);
            } else {
              throw new IllegalArgumentException("Header does not have raw or zlib data");
            }
            Osmformat.PrimitiveBlock block = Osmformat.PrimitiveBlock.parseFrom(data);
            for (int i = 0; i < block.getPrimitivegroupCount(); i++) {
              Osmformat.PrimitiveGroup group = block.getPrimitivegroup(i);
              stats.nodes += group.getDense().getIdCount();
              stats.nodes += group.getNodesCount();
              stats.ways += group.getWaysCount();
              stats.rels += group.getRelationsCount();
            }
          }
        });
    pipeline.awaitAndLog(ProgressLoggers.create()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats(), Duration.ofSeconds(10));

    System.err.println(countList.stream().reduce(Counts::sum));
  }

  public static void benchmarkJustPbfIoThreads(String file) throws Exception {
    var path = Path.of(file);
    var countList = new CopyOnWriteArrayList<Counts>();
    record Segment(long start, int length) {}

    var pipeline =
      WorkerPipeline.start("test", Stats.inMemory())
        .<Segment>fromGenerator("reader", (next) -> {
          try (var channel = FileChannel.open(path, READ)) {
            while (channel.position() < channel.size()) {
              ByteBuffer buf = ByteBuffer.allocate(4);
              channel.read(buf);
              buf = ByteBuffer.allocate(buf.flip().getInt());
              channel.read(buf);
              Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(buf.flip());
              long start = channel.position();
              int length = header.getDatasize();
              channel.position(channel.position() + length);
              if ("OSMData".equals(header.getType())) {
                next.accept(new Segment(start, length));
              }
            }
          } catch (EOFException e) {
            // done
          }
        }).addBuffer("buffer", 100)
        .sinkTo("counter", cpus, prev -> {
          Counts stats = new Counts();
          countList.add(stats);

          Segment seg;
          try (var channel = FileChannel.open(path, READ)) {
            while ((seg = prev.get()) != null) {
              channel.position(seg.start);
              ByteBuffer buf = ByteBuffer.allocate(seg.length);
              channel.read(buf);
              Fileformat.Blob blob = Fileformat.Blob.parseFrom(buf.flip());
              ByteString data;
              if (blob.hasRaw()) {
                data = blob.getRaw();
              } else if (blob.hasZlibData()) {
                byte[] buf2 = new byte[blob.getRawSize()];
                Inflater decompresser = new Inflater();
                decompresser.setInput(blob.getZlibData().toByteArray());
                decompresser.inflate(buf2);
                decompresser.end();
                data = ByteString.copyFrom(buf2);
              } else {
                throw new IllegalArgumentException("Header does not have raw or zlib data");
              }
              Osmformat.PrimitiveBlock block = Osmformat.PrimitiveBlock.parseFrom(data);
              for (int i = 0; i < block.getPrimitivegroupCount(); i++) {
                Osmformat.PrimitiveGroup group = block.getPrimitivegroup(i);
                stats.nodes += group.getDense().getIdCount();
                stats.nodes += group.getNodesCount();
                stats.ways += group.getWaysCount();
                stats.rels += group.getRelationsCount();
              }
            }
          }
        });
    pipeline.awaitAndLog(ProgressLoggers.create()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats(), Duration.ofSeconds(10));

    System.err.println(countList.stream().reduce(Counts::sum));
  }

  private static class NodeLocs {

    static final int segmentBits = 27; // 128MB
    static final int maxPendingSegments = 8; // 1GB
    static final long segmentMask = (1L << segmentBits) - 1;
    static final long segmentBytes = 1 << segmentBits;
    final Path path;

    MappedByteBuffer[] segments;

    NodeLocs(Path path) {
      this.path = path;
    }

    void init() {
      try {
        var channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        long outIdx = channel.size() + 8;
        int segmentCount = (int) (outIdx / segmentBytes + 1);
        segments = new MappedByteBuffer[segmentCount];
        int i = 0;
        for (long segmentStart = 0; segmentStart < outIdx; segmentStart += segmentBytes) {
          long segmentLength = Math.min(segmentBytes, outIdx - segmentStart);
          MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_ONLY, segmentStart, segmentLength);
          try {
            NativePosixUtil.madvise(buffer, NativePosixUtil.RANDOM);
          } catch (IOException | UnsatisfiedLinkError | NoClassDefFoundError e) {
          }
          segments[i++] = buffer;
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    long getLong(long index) {
      long byteOffset = index << 3;
      int idx = (int) (byteOffset >>> segmentBits);
      int offset = (int) (byteOffset & segmentMask);
      return segments[idx].getLong(offset);
    }
  }

  static class Bounds {

    double minX = Double.MAX_VALUE;
    double minY = Double.MAX_VALUE;
    double maxX = -Double.MAX_VALUE;
    double maxY = -Double.MAX_VALUE;

    public static Bounds sum(Bounds a, Bounds b) {
      var c = new Bounds();
      c.minX = Math.min(a.minX, b.minX);
      c.minY = Math.min(a.minY, b.minY);
      c.maxX = Math.max(a.maxX, b.maxX);
      c.maxY = Math.max(a.maxY, b.maxY);
      return c;
    }

    @Override
    public String toString() {
      return "Bounds{" +
        "minX=" + minX +
        ", minY=" + minY +
        ", maxX=" + maxX +
        ", maxY=" + maxY +
        '}';
    }

    void handle(double x, double y) {
      if (x < minX) {
        minX = x;
      }
      if (y < minY) {
        minY = y;
      }
      if (x > maxX) {
        maxX = x;
      }
      if (y > maxY) {
        maxY = y;
      }
    }
  }

  private static int readInt(FileChannel channel) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(4);
    channel.read(buf);
    return buf.flip().getInt();
  }

  private static ByteBuffer readByteBuffer(FileChannel channel, int bytes) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(bytes);
    channel.read(buf);
    return buf.flip();
  }

  private static ByteBuffer readByteBuffer(FileChannel channel, long offset, int bytes) throws IOException {
    channel.position(offset);
    return readByteBuffer(channel, bytes);
  }

  public static void benchmarkParallelNodeCache(String file) throws IOException {
    var path = Path.of(file);
    var tmpPath = Path.of("nodecache");
    record Segment(long start, int length) {}
    record ToFlush(ByteBuffer buf, long offset) {}
    CopyOnWriteArrayList<AtomicLong> segments = new CopyOnWriteArrayList<>();
    final ConcurrentMap<Long, ByteBuffer> writeBuffers = new ConcurrentHashMap<>();
    FileUtils.delete(tmpPath);
    FileUtils.deleteOnExit(tmpPath);
    var nodeCache = new NodeLocs(tmpPath);
    FileChannel chan = FileChannel.open(tmpPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
    Semaphore limitInMemoryChunks = new Semaphore(NodeLocs.maxPendingSegments);
    CountDownLatch nodesWritten = new CountDownLatch(1);
    CountDownLatch nodesProcessed = new CountDownLatch(cpus);
    var chunkQueue = new WorkQueue<ToFlush>("flush", NodeLocs.maxPendingSegments, 1, Stats.inMemory());
    var allBounds = new CopyOnWriteArrayList<Bounds>();
    AtomicLong blocks = new AtomicLong(0);
    AtomicLong nodes = new AtomicLong(0);
    AtomicLong ways = new AtomicLong(0);
    AtomicLong rels = new AtomicLong(0);

    var pipeline = WorkerPipeline.start("parallelNodeCache", Stats.inMemory())
      .<Segment>fromGenerator("read", (next) -> {
        try (FileChannel channel = FileChannel.open(path, READ)) {
          while (channel.position() < channel.size()) {
            var blockSize = readInt(channel);
            var buf = readByteBuffer(channel, blockSize);
            Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(buf);
            long start = channel.position();
            int length = header.getDatasize();
            channel.position(channel.position() + length);
            if ("OSMData".equals(header.getType())) {
              next.accept(new Segment(start, length));
            }
          }
        }
      }, 1)
      .addBuffer("buffer", 100, 1)
      .sinkTo("count", cpus, (elements) -> {
        var currentSeg = new AtomicLong(-1);
        segments.add(currentSeg);
        Bounds bounds = new Bounds();
        allBounds.add(bounds);
        boolean nodesDone = false;
        long lastSegment = -1;
        long segmentOffset = -1;
        ByteBuffer buffer = null;
        try (var channel = FileChannel.open(path, READ)) {
          for (Segment seg : IterableOnce.of(elements)) {
            ByteBuffer buf = readByteBuffer(channel, seg.start, seg.length);
            final Counts stats = new Counts();
            for (OsmElement elem : PbfDecoder.decode(buf.flip().array())) {
              if (elem instanceof OsmElement.Node node) {
                stats.nodes++;
                long offset = node.id() << 3;
                long segment = offset >>> NodeLocs.segmentBits;
                if (segment > lastSegment) {
                  synchronized (writeBuffers) {
                    currentSeg.set(segment);
                    var minSegment = segments.stream().mapToLong(AtomicLong::get).min().getAsLong();
                    for (Long key : writeBuffers.keySet()) {
                      if (key < minSegment) {
                        // no one else needs this segment, flush it
                        var toFlush = writeBuffers.remove(key);
                        if (toFlush != null) {
                          chunkQueue.accept(new ToFlush(toFlush, key << NodeLocs.segmentBits));
                        }
                      }
                    }
                    buffer = writeBuffers.computeIfAbsent(segment, i -> {
                      try {
                        limitInMemoryChunks.acquire();
                      } catch (InterruptedException e) {
                        e.printStackTrace();
                        throw new RuntimeException(e);
                      }
                      return ByteBuffer.allocateDirect(1 << NodeLocs.segmentBits);
                    });
                  }
                  lastSegment = segment;
                  segmentOffset = segment << NodeLocs.segmentBits;
                }
                buffer.putLong((int) (offset - segmentOffset), GeoUtils.packLatLon(node.lon(), node.lat()));
                stats.nodes++;
              } else if (elem instanceof OsmElement.Way way) {
                // make sure all nodes have been written to the node cache first...
                if (!nodesDone) {
                  try {
                    nodesProcessed.countDown();
                    nodesProcessed.await();
                    synchronized (writeBuffers) {
                      for (Long key : writeBuffers.keySet()) {
                        // no one else needs this segment, flush it
                        var toFlush = writeBuffers.remove(key);
                        if (toFlush != null) {
                          chunkQueue.accept(new ToFlush(toFlush, key << NodeLocs.segmentBits));
                        }
                      }
                    }
                    chunkQueue.close();
                    nodesWritten.await();
                  } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                  }
                  nodesDone = true;
                }
                var ns = way.nodes();
                for (int i = 0; i < ns.size(); i++) {
                  var node = ns.get(i);
                  long location = nodeCache.getLong(node);
                  double y = GeoUtils.unpackLat(location);
                  double x = GeoUtils.unpackLon(location);
                  if (location == 0) {
                    throw new IllegalStateException("Node location for node " + node);
                  }

                  bounds.handle(x, y);
                }
                stats.ways++;
              } else if (elem instanceof OsmElement.Relation) {
                stats.rels++;
              }
            }

            blocks.incrementAndGet();
            nodes.addAndGet(stats.nodes);
            ways.addAndGet(stats.ways);
            rels.addAndGet(stats.rels);
          }
        }
      });

    // spawn a separate worker to flush finished node location chunks to disk
    var flush = WorkerPipeline.start("flusher", Stats.inMemory())
      .readFromQueue(chunkQueue)
      .sinkTo("flusher", 1, prev -> {
        for (var buf : IterableOnce.of(prev)) {
          try {
            chan.write(buf.buf, buf.offset);
            buf.buf.clear();
            limitInMemoryChunks.release();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }
        nodeCache.init();
        nodesWritten.countDown();
      });
    ProgressLoggers.create()
      .addRateCounter("blocks", blocks)
      .addRateCounter("nodes", nodes)
      .addRateCounter("ways", ways)
      .addRateCounter("rels", rels)
      .newLine()
      .addPipelineStats(pipeline)
      .addPipelineStats(flush)
      .newLine()
      .addFileSize(tmpPath)
      .newLine()
      .addProcessStats()
      .awaitAndLog(Worker.joinFutures(pipeline.done(), flush.done()), Duration.ofSeconds(10));

    System.err.println(Format.defaultInstance().storage(FileUtils.size(tmpPath), false));
    System.err.println("nodes=" + nodes + " ways=" + ways + " rels=" + rels);
    System.err.println(allBounds.stream().reduce(Bounds::sum));
  }

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      // local testing
      while (true) {
        time(() -> benchmarkParallelNodeCache("data/sources/northeast.osm.pbf"));
      }
    }
    String file = args[1];
    switch (args[0]) {
      case "graphhopper" -> time(() -> benchmarkExistingGraphhopper(file));
      case "just-pbf" -> time(() -> benchmarkJustPbf(file));
      case "just-pbf-iothreads" -> time(() -> benchmarkJustPbfIoThreads(file));
      case "new" -> time(() -> benchmarkNew(file));
      case "new-iothreads" -> time(() -> benchmarkNewIoThreads(file));
      case "nodecache" -> time(() -> benchmarkParallelNodeCache(file));
    }
  }

  public static void time(RunnableThatThrows task) throws Exception {
    for (int i = 0; i < 1; i++) {
      var timer = Timer.start();
      task.run();
      System.err.println(timer.stop());
      System.err.println("total GC time: " + ProcessInfo.getGcTime());
    }
  }
}
