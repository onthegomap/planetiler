package com.onthegomap.planetiler.copy;

import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.util.Gzip;
import java.util.function.UnaryOperator;

final class TileDataReEncoders {

  private TileDataReEncoders() {}

  static UnaryOperator<byte[]> create(TileCopyContext c) {
    // for now just one - but compose multiple as needed in the future (decompress, do something, compress)
    return reCompressor(c.inputCompression(), c.outputCompression());
  }

  private static UnaryOperator<byte[]> reCompressor(TileCompression inCompression, TileCompression outCompression) {
    if (inCompression == outCompression) {
      return b -> b;
    } else if (inCompression == TileCompression.GZIP && outCompression == TileCompression.NONE) {
      return Gzip::gunzip;
    } else if (inCompression == TileCompression.NONE && outCompression == TileCompression.GZIP) {
      return Gzip::gzip;
    } else if (inCompression == TileCompression.UNKNOWN && outCompression == TileCompression.GZIP) {
      return b -> Gzip.isZipped(b) ? b : Gzip.gzip(b);
    } else if (inCompression == TileCompression.UNKNOWN && outCompression == TileCompression.NONE) {
      return b -> Gzip.isZipped(b) ? Gzip.gunzip(b) : b;
    } else {
      throw new IllegalArgumentException("unhandled case: in=" + inCompression + " out=" + outCompression);
    }
  }
}
