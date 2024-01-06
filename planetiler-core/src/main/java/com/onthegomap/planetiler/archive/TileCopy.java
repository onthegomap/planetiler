package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.CommonConfigs;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.Hashing;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility to copy/convert tiles and metadata from one archive into another.
 * <p>
 * Example usages:
 *
 * <pre>
 * --input=tiles.mbtiles --output=tiles.mbtiles
 * --input=tiles.mbtiles --output=tiles.pmtiles --skip_empty=false
 * --input=tiles.pmtiles --output=tiles.mbtiles
 * --input=tiles.mbtiles --output=tiles/
 * --input=tiles.mbtiles --output=tiles.json --out_tile_compression=gzip
 * --input=tiles.mbtiles --output=tiles.csv --out_tile_compression=none
 * --input=tiles.mbtiles --output=tiles.proto
 * </pre>
 */
public class TileCopy {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileCopy.class);

  private final TileCopyConfig config;

  private final Counter.MultiThreadCounter tilesWrittenOverall = Counter.newMultiThreadCounter();


  TileCopy(TileCopyConfig config) {
    this.config = config;
  }

  public void run() throws IOException {

    if (!config.inArchive().exists()) {
      throw new IllegalArgumentException("the input archive does not exist");
    }

    config.outArchive().setup(config.force(), config.append(), config.tileWriterThreads());

    final var loggers = ProgressLoggers.create()
      .addRateCounter("tiles", tilesWrittenOverall::get);

    try (
      var reader = TileArchives.newReader(config.inArchive(), config.inArguments());
      var writer = TileArchives.newWriter(config.outArchive(), config.outArguments())
    ) {

      final TileArchiveMetadata inMetadata = getInMetadata(reader);
      final TileArchiveMetadata outMetadata = getOutMetadata(inMetadata);

      writer.initialize();
      try (
        var rawTiles = reader.getAllTiles();
        var it = config.skipEmpty() ? rawTiles.filter(t -> t.bytes() != null && t.bytes().length > 0) : rawTiles
      ) {

        final var tileConverter = tileConverter(inMetadata.tileCompression(), outMetadata.tileCompression(), writer);
        var pipeline = WorkerPipeline.start("archive", config.stats())
          .readFrom("tiles", () -> it)
          .addBuffer("buffer", config.queueSize())
          .sinkTo("write", config.tileWriterThreads(), itt -> tileWriter(writer, itt, tileConverter));

        final var f = pipeline.done().thenRun(() -> writer.finish(outMetadata));

        loggers.awaitAndLog(f, config.logInterval());
      }
    }
  }

  private void tileWriter(WriteableTileArchive archive, Iterable<Tile> itt,
    Function<Tile, TileEncodingResult> tileConverter) {

    final Counter tilesWritten = tilesWrittenOverall.counterForThread();
    try (var tileWriter = archive.newTileWriter()) {
      for (Tile t : itt) {
        tileWriter.write(tileConverter.apply(t));
        tilesWritten.inc();
      }
    }
  }

  private static Function<Tile, TileEncodingResult> tileConverter(TileCompression inCompression,
    TileCompression outCompression, WriteableTileArchive writer) {

    final UnaryOperator<byte[]> bytesReEncoder = bytesReEncoder(inCompression, outCompression);
    final Function<byte[], OptionalLong> hasher =
      writer.deduplicates() ? b -> OptionalLong.of(Hashing.fnv1a64(b)) :
        b -> OptionalLong.empty();

    return t -> new TileEncodingResult(t.coord(), bytesReEncoder.apply(t.bytes()), hasher.apply(t.bytes()));
  }

  private static UnaryOperator<byte[]> bytesReEncoder(TileCompression inCompression, TileCompression outCompression) {
    if (inCompression == outCompression) {
      return UnaryOperator.identity();
    } else if (inCompression == TileCompression.GZIP && outCompression == TileCompression.NONE) {
      return Gzip::gunzip;
    } else if (inCompression == TileCompression.NONE && outCompression == TileCompression.GZIP) {
      return Gzip::gzip;
    } else if (inCompression == TileCompression.UNKNWON && outCompression == TileCompression.GZIP) {
      return b -> Gzip.isZipped(b) ? b : Gzip.gzip(b);
    } else if (inCompression == TileCompression.UNKNWON && outCompression == TileCompression.NONE) {
      return b -> Gzip.isZipped(b) ? Gzip.gunzip(b) : b;
    } else {
      throw new IllegalArgumentException("unhandled case: in=" + inCompression + " out=" + outCompression);
    }
  }

  private TileArchiveMetadata getInMetadata(ReadableTileArchive reader) {
    TileArchiveMetadata inMetadata = config.inMetadata();
    if (inMetadata == null) {
      inMetadata = reader.metadata();
      if (inMetadata == null) {
        LOGGER.atWarn()
          .log("the input archive does not contain any metadata using fallback - consider passing one via in_metadata");
        inMetadata = fallbackMetadata();
      }
    }
    if (inMetadata.tileCompression() == null) {
      inMetadata = inMetadata.withTileCompression(config.inCompression());
    }

    return inMetadata;
  }

  private TileArchiveMetadata getOutMetadata(TileArchiveMetadata inMetadata) {
    if (config.outCompression() == TileCompression.UNKNWON && inMetadata.tileCompression() == TileCompression.UNKNWON) {
      return inMetadata.withTileCompression(TileCompression.GZIP);
    } else if (config.outCompression() != TileCompression.UNKNWON) {
      return inMetadata.withTileCompression(config.outCompression());
    } else {
      return inMetadata;
    }
  }

  private static TileArchiveMetadata fallbackMetadata() {
    return new TileArchiveMetadata(
      "unknown",
      null,
      null,
      null,
      null,
      TileArchiveMetadata.MVT_FORMAT, // have to guess here that it's pbf
      null,
      null,
      null,
      null,
      new TileArchiveMetadata.TileArchiveMetadataJson(List.of()), // cannot provide any vector layers
      Map.of(),
      null
    );
  }

  record TileCopyConfig(
    TileArchiveConfig inArchive,
    TileArchiveConfig outArchive,
    Arguments inArguments,
    Arguments outArguments,
    TileCompression inCompression,
    TileCompression outCompression,
    int tileWriterThreads,
    Duration logInterval,
    Stats stats,
    int queueSize,
    boolean append,
    boolean force,
    TileArchiveMetadata inMetadata,
    boolean skipEmpty
  ) {
    static TileCopyConfig fromArguments(Arguments baseArguments) {

      final Arguments inArguments = baseArguments.withPrefix("in");
      final Arguments outArguments = baseArguments.withPrefix("out");
      final Arguments baseOrOutArguments = outArguments.orElse(baseArguments);

      final Path inMetadataPath = inArguments.file("metadata", "path to metadata.json to use instead", null);
      final TileArchiveMetadata inMetadata;
      if (inMetadataPath != null) {
        try {
          inMetadata =
            TileArchiveMetadataDeSer.mbtilesMapper().readValue(inMetadataPath.toFile(), TileArchiveMetadata.class);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      } else {
        inMetadata = null;
      }

      return new TileCopyConfig(
        TileArchiveConfig.from(baseArguments.getString("input", "input tile archive")),
        TileArchiveConfig.from(baseArguments.getString("output", "output tile archive")),
        inArguments,
        outArguments,
        getTileCompressionArg(inArguments, "the input tile compression"),
        getTileCompressionArg(outArguments, "the output tile compression"),
        CommonConfigs.tileWriterThreads(baseOrOutArguments),
        CommonConfigs.logInterval(baseArguments),
        baseArguments.getStats(),
        Math.max(100, (int) (5_000d * ProcessInfo.getMaxMemoryBytes() / 100_000_000_000d)),
        CommonConfigs.appendToArchive(baseOrOutArguments),
        CommonConfigs.force(baseOrOutArguments),
        inMetadata,
        baseArguments.getBoolean("skip_empty", "skip empty (null/zero-bytes) tiles", false)
      );
    }
  }

  private static TileCompression getTileCompressionArg(Arguments args, String description) {
    return args.getObject("tile_compression", description, TileCompression.UNKNWON,
      v -> TileCompression.findById(v).orElseThrow());
  }

  public static void main(String[] args) throws IOException {
    new TileCopy(TileCopyConfig.fromArguments(Arguments.fromEnvOrArgs(args))).run();
  }
}
