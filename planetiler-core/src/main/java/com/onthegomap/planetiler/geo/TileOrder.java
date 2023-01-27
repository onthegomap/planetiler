package com.onthegomap.planetiler.geo;

import java.util.function.IntFunction;
import java.util.function.ToDoubleBiFunction;
import java.util.function.ToIntFunction;

/**
 * Controls the sort order of {@link com.onthegomap.planetiler.collection.FeatureGroup}, which determines the ordering
 * of {@link com.onthegomap.planetiler.writer.TileEncodingResult}s when written to
 * {@link com.onthegomap.planetiler.writer.TileArchive.TileWriter}.
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
}
