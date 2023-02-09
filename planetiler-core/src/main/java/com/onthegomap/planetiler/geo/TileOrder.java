package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Bounds;
import java.util.Iterator;
import java.util.function.IntFunction;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToIntFunction;

/**
 * Controls the sort order of {@link com.onthegomap.planetiler.collection.FeatureGroup}, which determines the ordering
 * of {@link com.onthegomap.planetiler.archive.TileEncodingResult}s when written to
 * {@link WriteableTileArchive.TileWriter}.
 */
public enum TileOrder {
  TMS(TileCoord::encoded, TileCoord::decode, TileCoord::progressOnLevel),
  HILBERT(TileCoord::hilbertEncoded, TileCoord::hilbertDecode, TileCoord::hilbertProgressOnLevel);

  private final ToIntFunction<TileCoord> encode;
  private final IntFunction<TileCoord> decode;
  private final ToDoubleBiFunction<TileCoord, TileExtents> progressOnLevel;

  private TileOrder(ToIntFunction<TileCoord> encode, IntFunction<TileCoord> decode,
    ToDoubleBiFunction<TileCoord, TileExtents> progressOnLevel) {
    this.encode = encode;
    this.decode = decode;
    this.progressOnLevel = progressOnLevel;
  }

  public int encode(TileCoord coord) {
    return encode.applyAsInt(coord);
  }

  public TileCoord decode(int encoded) {
    return decode.apply(encoded);
  }

  public double progressOnLevel(TileCoord coord, TileExtents extents) {
    return progressOnLevel.applyAsDouble(coord, extents);
  }

  public Iterator<TileCoord> enumerate(int minzoom, int maxzoom, Bounds bounds) {
    int end = TileCoord.startIndexForZoom(maxzoom) + (1 << (2 * maxzoom));
    var result = new Iterator<TileCoord>() {
      int id = TileCoord.startIndexForZoom(minzoom);
      TileCoord next = decode(id);

      @Override
      public boolean hasNext() {
        return id < end;
      }

      @Override
      public TileCoord next() {
        var result = next;
        advance();
        return result;
      }

      private void advance() {
        do {
          id++;
        } while (id < end && !bounds.tileExtents().test(next = decode(id)));
      }
    };
    if (!bounds.tileExtents().test(decode(result.id))) {
      result.advance();
    }
    return result;
  }
}
