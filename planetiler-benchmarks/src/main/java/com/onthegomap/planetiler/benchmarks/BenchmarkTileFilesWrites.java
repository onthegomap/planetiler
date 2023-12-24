package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.files.TileSchemeEncoding;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

/**
 * Loads a mbtiles file into memory, and writes the tiles to disk in different modes.
 */
public class BenchmarkTileFilesWrites {

  public static void main(String[] args) throws Exception {

    final Arguments options = Arguments.fromArgs(args);

    final Path mbtilesFile =
      Paths.get(options.getString("benchmark_mbtiles", "path to a mbtiles file that will be loaded into memory"));

    final int runs = options.getInteger("benchmark_runs", "the number of runs per type", 2);

    final String tmpOutputPath =
      options.getString("benchmark_tmp_output", "the directory tiles will be written to - temporarily", null);


    final List<TileEncodingResult> tiles;
    try (var db = Mbtiles.newReadOnlyDatabase(mbtilesFile)) {
      System.out.println();
      System.out.println("loading mbtiles into memory...");
      tiles = db.getAllTiles().stream()
        .map(t -> new TileEncodingResult(t.coord(), t.bytes(), OptionalLong.empty()))
        .toList();
    }

    final List<Function<Function<TileCoord, Path>, TilesWriter>> tileWriterFactories = List.of(
      tse -> new BoundTilesWriter("fixed1", tiles, 1, tse, Executors.newFixedThreadPool(1)),
      tse -> new BoundTilesWriter("fixed2", tiles, 2, tse, Executors.newFixedThreadPool(2)),
      tse -> new BoundTilesWriter("fixed4", tiles, 4, tse, Executors.newFixedThreadPool(4)),
      tse -> new BoundTilesWriter("fixed10Virtual", tiles, 10, tse, Executors.newVirtualThreadPerTaskExecutor()),
      tse -> new BoundTilesWriter("fixed100Virtual", tiles, 100, tse, Executors.newVirtualThreadPerTaskExecutor()),
      tse -> new BoundTilesWriter("fixed1000Virtual", tiles, 1000, tse, Executors.newVirtualThreadPerTaskExecutor()),
      tse -> new AsyncTilesWriter("async", tiles, tse),
      tse -> new UnboundVirtualTilesWriter("unboundVirtual", tiles, tse)
    );

    for (Function<Function<TileCoord, Path>, TilesWriter> tileWriterFactory : tileWriterFactories) {
      for (int run = 0; run < runs; run++) {
        final Path p = tmpOutputPath == null ? Files.createTempDirectory("benchmark") :
          Files.createTempDirectory(Paths.get(tmpOutputPath), "benchmark");
        p.toFile().deleteOnExit();

        Timer timer;
        try {
          TilesWriter tilesWriter = tileWriterFactory.apply(new TileSchemeEncoding("{z}/{xs}/{ys}.pbf", p).encoder());
          System.out.println();
          System.out.println("#" + run + " " + tilesWriter.name());
          timer = Timer.start();
          tilesWriter.run();
          timer.stop();
          System.out.println(timer.elapsed());
        } finally {
          System.out.println("cleaning directory...");
          FileUtils.deleteDirectory(p);
        }
      }
    }
  }

  private static Path pathForTile(TileEncodingResult tile, Function<TileCoord, Path> tileSchemeEncoder,
    Path prevTilePath) {
    final Path tilePath = tileSchemeEncoder.apply(tile.coord());
    final Path tileFolder = tilePath.getParent();
    if (prevTilePath == null || !tileFolder.equals(prevTilePath.getParent())) {
      try {
        Files.createDirectories(tileFolder);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return tilePath;
  }

  private interface TilesWriter {
    void run() throws Exception;

    String name();
  }

  private static class BoundTilesWriter implements TilesWriter {

    private final String name;

    private final int writers;

    private final Function<TileCoord, Path> tileSchemeEncoder;

    private final ExecutorService executor;

    private final CompletableFuture[] futures;

    private final ConcurrentLinkedQueue<TileEncodingResult> queue;

    public BoundTilesWriter(String name, List<TileEncodingResult> tiles, int writers,
      Function<TileCoord, Path> tileSchemeEncoder, ExecutorService executor) {
      this.name = name;
      this.writers = writers;
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.executor = executor;
      this.futures = new CompletableFuture[writers];
      this.queue = new ConcurrentLinkedQueue<>(tiles);
    }

    @Override
    public void run() throws Exception {
      for (int i = 0; i < writers; i++) {
        futures[i] = CompletableFuture.runAsync(this::writeTiles, executor);
      }
      CompletableFuture.allOf(futures).get();
      executor.shutdownNow();
    }

    @Override
    public String name() {
      return name;
    }

    private void writeTiles() {
      TileEncodingResult tile;
      Path prevTilePath = null;
      while ((tile = queue.poll()) != null) {
        Path tilePath = pathForTile(tile, tileSchemeEncoder, prevTilePath);
        prevTilePath = tilePath;
        try {
          Files.write(tilePath, tile.tileData());
        } catch (IOException ioe) {
          throw new UncheckedIOException(ioe);
        }
      }
    }
  }

  private static class AsyncTilesWriter implements TilesWriter {

    private final String name;
    private final Function<TileCoord, Path> tileSchemeEncoder;
    private final List<TileEncodingResult> tiles;

    private final CountDownLatch doneSignal;

    public AsyncTilesWriter(String name, List<TileEncodingResult> tiles, Function<TileCoord, Path> tileSchemeEncoder) {
      this.name = name;
      this.tiles = tiles;
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.doneSignal = new CountDownLatch(tiles.size());
    }

    @Override
    public void run() throws InterruptedException {
      Path prevTilePath = null;
      for (TileEncodingResult tile : tiles) {
        final Path tilePath = pathForTile(tile, tileSchemeEncoder, prevTilePath);
        prevTilePath = tilePath;
        try {
          @SuppressWarnings("java:S2095") AsynchronousFileChannel asyncFile =
            AsynchronousFileChannel.open(tilePath, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

          asyncFile.write(ByteBuffer.wrap(tile.tileData()), 0, doneSignal,
            new CompletionHandler<>() {
              @Override
              public void completed(Integer integer, CountDownLatch cDoneSignal) {
                cDoneSignal.countDown();
                try {
                  asyncFile.close();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              }

              @Override
              public void failed(Throwable throwable, CountDownLatch cDoneSignal) {
                cDoneSignal.countDown();
                try {
                  asyncFile.close();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              }
            });
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }

      doneSignal.await();
    }

    @Override
    public String name() {
      return name;
    }
  }

  private static class UnboundVirtualTilesWriter implements TilesWriter {

    private final String name;
    private final List<TileEncodingResult> tiles;
    private final Function<TileCoord, Path> tileSchemeEncoder;

    private final ExecutorService executor;

    private final CountDownLatch doneSignal;

    public UnboundVirtualTilesWriter(String name, List<TileEncodingResult> tiles,
      Function<TileCoord, Path> tileSchemeEncoder) {
      this.name = name;
      this.tiles = tiles;
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.executor = Executors.newVirtualThreadPerTaskExecutor();
      this.doneSignal = new CountDownLatch(tiles.size());
    }

    @Override
    public void run() throws InterruptedException {
      for (TileEncodingResult tile : tiles) {
        CompletableFuture.runAsync(() -> writeTile(tile), executor);
      }
      doneSignal.await();
      executor.shutdownNow();
    }

    @Override
    public String name() {
      return name;
    }

    private void writeTile(TileEncodingResult tile) {
      final Path tilePath = pathForTile(tile, tileSchemeEncoder, null);
      try {
        Files.write(tilePath, tile.tileData());
        doneSignal.countDown();
      } catch (IOException ioe) {
        throw new UncheckedIOException(ioe);
      }
    }
  }
}
