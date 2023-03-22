package com.onthegomap.planetiler.pmtiles;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.ScannableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ReadablePmtiles implements ReadableTileArchive, ScannableTileArchive {
  private final SeekableByteChannel channel;
  private final Pmtiles.Header header;
  private final LoadingCache<OffsetAndLength, List<Pmtiles.Entry>> directoryCache;

  public ReadablePmtiles(SeekableByteChannel channel) throws IOException {
    this(channel, Arguments.of());
  }

  public ReadablePmtiles(SeekableByteChannel channel, Arguments arguments) throws IOException {
    this.channel = channel;
    this.header = Pmtiles.Header.fromBytes(getBytes(0, Pmtiles.HEADER_LEN));
    int cacheSizeMb = arguments.getInteger("cache_size_mb", "pmtiles: cache size for directories in megabytes", 500);
    this.directoryCache = CacheBuilder.newBuilder()
      .maximumWeight(cacheSizeMb * 1_000_000L)
      .weigher((OffsetAndLength k, List<Pmtiles.Entry> v) -> v.size() * (Pmtiles.Entry.BYTES + 8))
      .build(CacheLoader.from(key -> {
        try {
          var buf = getBytes(key.offset, key.length);
          if (header.internalCompression() == Pmtiles.Compression.GZIP) {
            buf = Gzip.gunzip(buf);
          }
          return Pmtiles.directoryFromBytes(buf);
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }));
  }

  public static ReadableTileArchive newReadFromFile(Path path, Arguments arguments) throws IOException {
    return new ReadablePmtiles(FileChannel.open(path, StandardOpenOption.READ), arguments);
  }

  /**
   * Finds the relevant entry for a tileId in a list of entries.
   * <p>
   * If there is an exact match for tileId, return that. Else if the tileId matches an entry's tileId + runLength,
   * return that. Else if the preceding entry is a directory (runLength = 0), return that. Else return null.
   */
  public static Pmtiles.Entry findTile(List<Pmtiles.Entry> entries, long tileId) {
    int m = 0;
    int n = entries.size() - 1;
    while (m <= n) {
      int k = (n + m) >> 1;
      long cmp = tileId - entries.get(k).tileId();
      if (cmp > 0) {
        m = k + 1;
      } else if (cmp < 0) {
        n = k - 1;
      } else {
        return entries.get(k);
      }
    }
    if (n >= 0 && (entries.get(n).runLength() == 0 || tileId - entries.get(n).tileId() < entries.get(n).runLength())) {
      return entries.get(n);
    }
    return null;
  }

  private synchronized byte[] getBytes(long start, int length) throws IOException {
    channel.position(start);
    var buf = ByteBuffer.allocate(length);
    channel.read(buf);
    return buf.array();
  }

  @Override
  @SuppressWarnings("java:S1168")
  public byte[] getTile(int x, int y, int z) {
    try {
      var tileId = TileCoord.ofXYZ(x, y, z).hilbertEncoded();

      long dirOffset = header.rootDirOffset();
      int dirLength = (int) header.rootDirLength();

      for (int depth = 0; depth <= 3; depth++) {
        var dir = readDir(dirOffset, dirLength);
        var entry = findTile(dir, tileId);
        if (entry != null) {
          if (entry.runLength() > 0) {
            return getBytes(header.tileDataOffset() + entry.offset(), entry.length());
          } else {
            dirOffset = header.leafDirectoriesOffset() + entry.offset();
            dirLength = entry.length();
          }
        } else {
          return null;
        }

      }
    } catch (IOException e) {
      throw new IllegalStateException("Could not get tile", e);
    }

    return null;
  }

  public Pmtiles.Header getHeader() {
    return header;
  }

  public Pmtiles.JsonMetadata getJsonMetadata() throws IOException {
    var buf = getBytes(header.jsonMetadataOffset(), (int) header.jsonMetadataLength());
    if (header.internalCompression() == Pmtiles.Compression.GZIP) {
      buf = Gzip.gunzip(buf);
    }
    return Pmtiles.JsonMetadata.fromBytes(buf);
  }

  @Override
  public TileArchiveMetadata metadata() {
    try {
      var jsonMetadata = getJsonMetadata();
      var map = new LinkedHashMap<>(jsonMetadata.otherMetadata());
      return new TileArchiveMetadata(
        map.remove(TileArchiveMetadata.NAME_KEY),
        map.remove(TileArchiveMetadata.DESCRIPTION_KEY),
        map.remove(TileArchiveMetadata.ATTRIBUTION_KEY),
        map.remove(TileArchiveMetadata.VERSION_KEY),
        map.remove(TileArchiveMetadata.TYPE_KEY),
        switch (header.tileType()) {
        case MVT -> TileArchiveMetadata.MVT_FORMAT;
        default -> null;
        },
        header.bounds(),
        header.center(),
        (double) header.centerZoom(),
        (int) header.minZoom(),
        (int) header.maxZoom(),
        jsonMetadata.vectorLayers(),
        map
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private List<Pmtiles.Entry> readDir(long offset, int length) {
    return directoryCache.getUnchecked(new OffsetAndLength(offset, length));
  }

  // Warning: this will only work on z15 or less pmtiles which planetiler creates
  private Stream<TileCoord> getTileCoords(List<Pmtiles.Entry> dir) {
    return dir.stream().flatMap(entry -> entry.runLength() == 0 ?
      getTileCoords(readDir(header.leafDirectoriesOffset() + entry.offset(), entry.length())) : IntStream
        .range((int) entry.tileId(), (int) entry.tileId() + entry.runLength()).mapToObj(TileCoord::hilbertDecode));
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    List<Pmtiles.Entry> rootDir;
    rootDir = readDir(header.rootDirOffset(), (int) header.rootDirLength());
    return new TileCoordIterator(getTileCoords(rootDir));
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }

  private record OffsetAndLength(long offset, int length) {}

  private static class TileCoordIterator implements CloseableIterator<TileCoord> {
    private final Stream<TileCoord> stream;
    private final Iterator<TileCoord> iterator;

    public TileCoordIterator(Stream<TileCoord> stream) {
      this.stream = stream;
      this.iterator = stream.iterator();
    }

    @Override
    public void close() {
      stream.close();
    }

    @Override
    public boolean hasNext() {
      return this.iterator.hasNext();
    }

    @Override
    public TileCoord next() {
      return this.iterator.next();
    }
  }
}
