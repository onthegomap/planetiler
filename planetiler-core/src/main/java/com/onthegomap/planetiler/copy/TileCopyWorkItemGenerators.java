package com.onthegomap.planetiler.copy;

import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileExtents;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Hashing;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class TileCopyWorkItemGenerators {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileCopyWorkItemGenerators.class);

  private TileCopyWorkItemGenerators() {}

  @SuppressWarnings("java:S2095")
  static CloseableIterator<TileCopyWorkItem> create(TileCopyContext context) {

    final TileArchiveConfig.Format inFormat = context.config().inArchive().format();
    final TileArchiveConfig.Format outFormat = context.config().outArchive().format();

    final boolean inOrder;
    if (outFormat == TileArchiveConfig.Format.FILES) {
      inOrder = true; // always use the input formats native order when outputting files
    } else if (inFormat == TileArchiveConfig.Format.FILES) {
      inOrder = false; // never use the inOrder looper when using files since there's no order guarantee
    } else {
      inOrder = context.config().scanTilesInOrder();
    }

    final int minzoom = context.outMetadata().minzoom();
    final int maxzoom = context.outMetadata().maxzoom();
    final Envelope filterBounds =
      context.outMetadata().bounds() == null ? Bounds.WORLD.world() :
        GeoUtils.toWorldBounds(context.outMetadata().bounds());
    final var boundsFilter = TileExtents.computeFromWorldBounds(maxzoom, filterBounds);
    final Predicate<TileCopyWorkItem> zoomFilter = i -> i.getCoord().z() >= minzoom && i.getCoord().z() <= maxzoom;

    final ProcessorArgs processorArgs = new ProcessorArgs(
      TileDataReEncoders.create(context),
      context.writer().deduplicates() ? b -> OptionalLong.of(Hashing.fnv1a64(b)) :
        b -> OptionalLong.empty()
    );

    if (inOrder) {
      CloseableIterator<TileCopyWorkItem> it = new EagerInOrder(context.reader().getAllTiles(), processorArgs)
        .filter(zoomFilter);
      if (!Objects.equals(context.inMetadata().bounds(), context.outMetadata().bounds())) {
        it = it.filter(i -> boundsFilter.test(i.getCoord()));
      }
      return it;

    } else {

      final boolean warnPoorlySupported = switch (inFormat) {
        case CSV, TSV, PROTO, PBF, JSON -> true;
        case PMTILES, MBTILES, FILES -> false;
      };
      if (warnPoorlySupported) {
        LOGGER.atWarn().log("{} random access is very slow", inFormat.id());
      }

      final TileOrder tileOrder = Optional.ofNullable(context.config().outputTileOrder())
        .orElse(context.writer().tileOrder());

      return new TileOrderLoop(tileOrder, minzoom, maxzoom, context.reader()::getTile, processorArgs)
        .filter(i -> boundsFilter.test(i.getCoord()));
    }
  }

  private record EagerInOrder(
    CloseableIterator<Tile> it,
    ProcessorArgs processorArgs
  ) implements CloseableIterator<TileCopyWorkItem> {

    @Override
    public void close() {
      it.close();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public TileCopyWorkItem next() {
      if (!it.hasNext()) {
        throw new NoSuchElementException();
      }
      final Tile t = it.next();
      return new TileCopyWorkItem(t.coord(), t::bytes, processorArgs.reEncoder(), processorArgs.hasher());
    }
  }

  private static class TileOrderLoop implements CloseableIterator<TileCopyWorkItem> {

    private final TileOrder tileOrder;
    private final int max;
    private final Function<TileCoord, byte[]> dataLoader;
    private final ProcessorArgs processorArgs;
    private int current;

    TileOrderLoop(TileOrder tileOrder, int minZoom, int maxZoom, Function<TileCoord, byte[]> dataLoader,
      ProcessorArgs processorArgs) {
      this.tileOrder = tileOrder;
      this.current = TileCoord.startIndexForZoom(minZoom);
      this.max = TileCoord.endIndexForZoom(maxZoom);
      this.dataLoader = dataLoader;
      this.processorArgs = processorArgs;
    }

    @Override
    public boolean hasNext() {
      return current <= max;
    }

    @Override
    public TileCopyWorkItem next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      final TileCoord c = tileOrder.decode(current++);
      return new TileCopyWorkItem(c, () -> dataLoader.apply(c), processorArgs.reEncoder(), processorArgs.hasher());
    }

    @Override
    public void close() {
      // nothing to close
    }
  }

  private record ProcessorArgs(UnaryOperator<byte[]> reEncoder, Function<byte[], OptionalLong> hasher) {}
}
