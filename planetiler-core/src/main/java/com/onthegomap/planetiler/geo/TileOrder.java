package com.onthegomap.planetiler.geo;

import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

/**
 * Controls the sort order of {@link com.onthegomap.planetiler.collection.FeatureGroup}, which determines the ordering
 * of {@link com.onthegomap.planetiler.writer.TileEncodingResult}s when written to
 * {@link com.onthegomap.planetiler.writer.TileArchive.TileWriter}.
 */
public enum TileOrder {
  TMS(TileCoord::encoded, TileCoord::decode),
  HILBERT(TileCoord::hilbertEncoded, TileCoord::hilbertDecode);

  private final ToIntFunction<TileCoord> encode;
  private final IntFunction<TileCoord> decode;

  private TileOrder(ToIntFunction<TileCoord> encode, IntFunction<TileCoord> decode) {
    this.encode = encode;
    this.decode = decode;
  }

  public int encode(TileCoord coord) {
    return encode.applyAsInt(coord);
  }

  public TileCoord decode(int encoded) {
    return decode.apply(encoded);
  }
}
