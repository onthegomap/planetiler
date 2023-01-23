package com.onthegomap.planetiler.geo;

import java.util.function.IntFunction;
import java.util.function.ToIntFunction;

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
