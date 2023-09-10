package com.onthegomap.planetiler.pmtiles;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchives;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class ReadablePmtiles implements ReadableTileArchive {
  private final DataProvider channel;
  private final Pmtiles.Header header;
  private final LoadingCache<Range, byte[]> tileCache;
  private final LoadingCache<Range, List<Pmtiles.Entry>> dirCache;
  private final Closeable closeable;

  @FunctionalInterface
  interface DataProvider {
    byte[] getBytes(long offset, int length) throws IOException;
  }

  public ReadablePmtiles(DataProvider channel, Closeable closeable) {
    this.channel = channel;
    this.closeable = closeable;
    this.header = Pmtiles.Header.fromBytes(getBytes(0, Pmtiles.HEADER_LEN));
    this.tileCache = CacheBuilder.newBuilder()
      .weigher((Range key, byte[] value) -> value.length)
      .maximumWeight(1_000_000_000)
      .build(CacheLoader.from(range -> getBytes(range.offset, range.length)));
    this.dirCache = CacheBuilder.newBuilder()
      .weigher((Range key, List<Pmtiles.Entry> value) -> value.size() * (8 + 8 + 8 + 8 + 4 + 4))
      .maximumWeight(1_000_000_000)
      .build(CacheLoader.from(range -> getDir(range.offset, range.length)));
  }

  public static ReadablePmtiles newReadFromFile(Path path) throws IOException {
    var channel = FileChannel.open(path, StandardOpenOption.READ);
    return new ReadablePmtiles((start, length) -> {
      var buf = ByteBuffer.allocate(length);
      channel.read(buf, start);
      return buf.array();
    }, channel);
  }

  private byte[] getBytes(long start, int length) {
    try {
      return channel.getBytes(start, length);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
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

  private List<Pmtiles.Entry> getDir(long offset, int length) {
    try {
      byte[] dirBytes = getBytes(offset, length);
      if (header.internalCompression() == Pmtiles.Compression.GZIP) {
        dirBytes = Gzip.gunzip(dirBytes);
      }
      return Pmtiles.directoryFromBytes(dirBytes);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }


  @Override
  @SuppressWarnings("java:S1168")
  public byte[] getTile(int x, int y, int z) {
    try {
      var tileId = TileCoord.ofXYZ(x, y, z).hilbertEncoded();

      long dirOffset = header.rootDirOffset();
      int dirLength = (int) header.rootDirLength();

      for (int depth = 0; depth <= 3; depth++) {
        var dir = dirCache.get(new Range(dirOffset, dirLength));
        var entry = findTile(dir, tileId);
        if (entry != null) {
          if (entry.runLength() > 0) {
            return tileCache.get(new Range(header.tileDataOffset() + entry.offset(), entry.length()));
          } else {
            dirOffset = header.leafDirectoriesOffset() + entry.offset();
            dirLength = entry.length();
          }
        } else {
          return null;
        }

      }
    } catch (ExecutionException e) {
      throw new IllegalStateException("Could not get tile", e.getCause());
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

    TileCompression tileCompression = switch (header.tileCompression()) {
      case GZIP -> TileCompression.GZIP;
      case NONE -> TileCompression.NONE;
      case UNKNOWN -> TileCompression.UNKNWON;
    };

    String format = switch (header.tileType()) {
      case MVT -> TileArchiveMetadata.MVT_FORMAT;
      default -> null;
    };

    try {
      var jsonMetadata = getJsonMetadata();
      var map = new LinkedHashMap<>(jsonMetadata.otherMetadata());
      return new TileArchiveMetadata(
        map.remove(TileArchiveMetadata.NAME_KEY),
        map.remove(TileArchiveMetadata.DESCRIPTION_KEY),
        map.remove(TileArchiveMetadata.ATTRIBUTION_KEY),
        map.remove(TileArchiveMetadata.VERSION_KEY),
        map.remove(TileArchiveMetadata.TYPE_KEY),
        format,
        header.bounds(),
        header.center(),
        (double) header.centerZoom(),
        (int) header.minZoom(),
        (int) header.maxZoom(),
        jsonMetadata.vectorLayers(),
        map,
        tileCompression
      );
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class StreamIterator<T> implements CloseableIterator<T> {
    private final Stream<T> stream;
    private final Iterator<T> iterator;

    public StreamIterator(Stream<T> stream) {
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
    public T next() {
      return this.iterator.next();
    }
  }

  private List<Pmtiles.Entry> readDir(long offset, int length) {
    try {
      var buf = getBytes(offset, length);
      if (header.internalCompression() == Pmtiles.Compression.GZIP) {
        buf = Gzip.gunzip(buf);
      }
      return Pmtiles.directoryFromBytes(buf);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  // Warning: this will only work on z15 or less pmtiles which planetiler creates
  private Stream<TileCoord> getTileCoords(List<Pmtiles.Entry> dir) {
    return dir.stream().flatMap(entry -> entry.runLength() == 0 ?
      getTileCoords(readDir(header.leafDirectoriesOffset() + entry.offset(), entry.length())) : IntStream
        .range((int) entry.tileId(), (int) entry.tileId() + entry.runLength()).mapToObj(TileCoord::hilbertDecode));
  }

  private CloseableIterator<Tile> getTiles(List<Pmtiles.Entry> dir) {
    return new CloseableIterator<Tile>() {
      @Override
      public void close() {}

      final Deque<Iterator<Pmtiles.Entry>> stack = new LinkedList<>();
      Iterator<Pmtiles.Entry> entryIterator = dir.iterator();
      Tile next = null;
      long tileId = 0;
      int i = 0;
      int rep = 0;
      byte[] data = null;

      {
        stack.push(entryIterator);
        advance();
      }

      @Override
      public boolean hasNext() {
        return next != null;
      }

      void advance() {
        if (i < rep) {
          next = new Tile(TileCoord.hilbertDecode((int) (tileId + i)), data);
          i++;
          return;
        }
        rep = 0;
        next = null;

        while (!stack.isEmpty() && !stack.peek().hasNext()) {
          stack.pop();
          entryIterator = stack.peek();
        }
        if (entryIterator == null) {
          return;
        }
        while (true) {
          var entry = entryIterator.next();
          if (entry.runLength == 0) {
            entryIterator = readDir(header.leafDirectoriesOffset() + entry.offset(), entry.length()).iterator();
            stack.push(entryIterator);
          } else {
            this.data = getBytes(header.tileDataOffset() + entry.offset(), entry.length());
            this.tileId = entry.tileId();
            next = new Tile(TileCoord.hilbertDecode((int) tileId), data);
            this.i = 1;
            this.rep = entry.runLength;
            break;
          }
        }
      }

      @Override
      public Tile next() {
        Tile result = this.next;
        advance();
        return result;
      }
    };
    //    return dir.stream().mapMulti((entry, next) -> {
    //      try {
    //        if (entry.runLength == 0) {
    //          getTiles(readDir(header.leafDirectoriesOffset() + entry.offset(), entry.length())).forEach(next);
    //        } else {
    //          var data = getBytes(header.tileDataOffset() + entry.offset(), entry.length());
    //          for (int i = 0; i < entry.runLength(); i++) {
    //            next.accept(new Tile(TileCoord.hilbertDecode((int) (entry.tileId() + i)), data));
    //          }
    //        }
    //      } catch (IOException e) {
    //        throw new IllegalStateException(e);
    //      }
    //    });
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    List<Pmtiles.Entry> rootDir;
    rootDir = readDir(header.rootDirOffset(), (int) header.rootDirLength());
    return new StreamIterator<>(getTileCoords(rootDir));
  }

  private record Range(long offset, int length) {}

  @Override
  public CloseableIterator<Tile> getAllTiles() {
    List<Pmtiles.Entry> rootDir;
    rootDir = readDir(header.rootDirOffset(), (int) header.rootDirLength());
    return getTiles(rootDir);
  }

  public static void main(String[] args) throws IOException {
    try (
      var reader = TileArchives.newReader(Path.of("planet-stats.pmtiles"), PlanetilerConfig.defaults())
    ) {
      long start = System.currentTimeMillis();
      long bytes = 0;
      long i = 0;
      //      try (var iter = reader.getAllTileCoords()) {
      //        while (iter.hasNext()) {
      //          bytes += reader.getTile(iter.next()).length;
      //          if (++i % 10_000_000 == 0) {
      //            System.err.println("Read " + (i / 1000000) + "m tiles");
      //          }
      //        }
      //      }
      //      System.err.println("Read pmtiles1 " + bytes + " in " + (System.currentTimeMillis() - start) / 1000 + "s");
      start = System.currentTimeMillis();
      bytes = 0;
      i = 0;
      try (var iter = reader.getAllTiles()) {
        while (iter.hasNext()) {
          bytes += iter.next().bytes().length;
          if (++i % 10_000_000 == 0) {
            System.err.println("Read " + (i / 1000000) + "m tiles");
          }
        }
      }
      System.err.println("Read pmtiles2 " + bytes + " in " + (System.currentTimeMillis() - start) / 1000 + "s");
    }
  }

  @Override
  public void close() throws IOException {
    closeable.close();
  }
}
