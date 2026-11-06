package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.util.TileSizeStats;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * Encodes vector tiles to the bytes stored in a tile archive. Not thread-safe: each thread gets its own instance from
 * {@link TileEncoders#factory}, closes it when done, and must not reuse it after {@link #encode} throws.
 */
sealed interface TileEncoder extends AutoCloseable
  permits MvtTileEncoder, JavaMltTileEncoder, NativeMltTileEncoder {

  /** The encoding of a tile, and the size and statistics of each of its layers. */
  record Encoded(byte[] bytes, List<TileSizeStats.LayerStats> layerStats) {

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof Encoded other && Arrays.equals(bytes, other.bytes) &&
        Objects.equals(layerStats, other.layerStats));
    }

    @Override
    public int hashCode() {
      return 31 * Arrays.hashCode(bytes) + Objects.hashCode(layerStats);
    }

    @Override
    public String toString() {
      return "Encoded{bytes=byte[" + bytes.length + "], layerStats=" + layerStats + "}";
    }
  }

  /** Returns the encoding of {@code tile}, with feature IDs if {@code includeIds} is true. */
  Encoded encode(VectorTile tile, boolean includeIds);

  /** Releases the resources this encoder owns, if any. */
  @Override
  default void close() {}
}
