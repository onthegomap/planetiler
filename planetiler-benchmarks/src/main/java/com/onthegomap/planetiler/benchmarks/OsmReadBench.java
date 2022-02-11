package com.onthegomap.planetiler.benchmarks;

import static java.nio.file.StandardOpenOption.READ;

import com.google.protobuf.ByteString;
import com.graphhopper.reader.ReaderElement;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.PbfDecoder;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.worker.RunnableThatThrows;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.Inflater;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Osmformat;

public class OsmReadBench {

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


  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      // local testing
      while (true) {
        time(() -> benchmarkNew("data/sources/northeast.osm.pbf"));
      }
    }
    String file = args[1];
    switch (args[0]) {
      case "graphhopper" -> time(() -> benchmarkExistingGraphhopper(file));
      case "just-pbf" -> time(() -> benchmarkJustPbf(file));
      case "just-pbf-iothreads" -> time(() -> benchmarkJustPbfIoThreads(file));
      case "new" -> time(() -> benchmarkNew(file));
      case "new-iothreads" -> time(() -> benchmarkNewIoThreads(file));
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
