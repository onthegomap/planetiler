package com.onthegomap.planetiler.pmtiles;

import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.Gzip;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReadablePmtiles implements ReadableTileArchive {
  private SeekableByteChannel channel;

  public ReadablePmtiles(SeekableByteChannel channel) {
    this.channel = channel;
  }

  /**
   * Finds the relevant entry for a tileId in a list of entries.
   * <p>
   * If there is an exact match for tileId, return that. Else if the tileId matches an entry's tileId + runLength,
   * return that. Else if the preceding entry is a directory (runLength = 0), return that. Else return null.
   */
  public static WriteablePmtiles.Entry findTile(List<WriteablePmtiles.Entry> entries, long tileId) {
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

    if (n >= 0) {
      if (entries.get(n).runLength() == 0 || tileId - entries.get(n).tileId() < entries.get(n).runLength()) {
        return entries.get(n);
      }
    }
    return null;
  }

  @Override
  public byte[] getTile(int x, int y, int z) {
    try {
      var tileId = TileCoord.ofXYZ(x, y, z).hilbertEncoded();
      var header = getHeader();

      long dirOffset = header.rootDirOffset();
      long dirLength = header.rootDirLength();

      for (int depth = 0; depth <= 3; depth++) {
        this.channel.position(dirOffset);
        var buf = ByteBuffer.allocate((int) dirLength);
        this.channel.read(buf);

        byte[] dirBytes = buf.array();
        if (header.internalCompression() == WriteablePmtiles.Compression.GZIP) {
          dirBytes = Gzip.gunzip(dirBytes);
        }

        var dir = WriteablePmtiles.deserializeDirectory(dirBytes);
        var entry = findTile(dir, tileId);
        if (entry != null) {
          if (entry.runLength() > 0) {
            var buf2 = ByteBuffer.allocate(entry.length());
            this.channel.position(header.tileDataOffset() + entry.offset());
            this.channel.read(buf2);
            return buf2.array();
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

  public WriteablePmtiles.Header getHeader() throws IOException {
    this.channel.position(0);
    var buf = ByteBuffer.allocate(127);
    this.channel.read(buf);
    return WriteablePmtiles.Header.fromBytes(buf.array());
  }

  private class TileCoordIterator implements CloseableIterator<TileCoord> {
    private Iterator<TileCoord> iter;

    @Override
    public void close() {
      // noop
    }

    @Override
    public boolean hasNext() {
      return iter.hasNext();
    }

    @Override
    public TileCoord next() {
      return iter.next();
    }

    private void collectTileCoords(List<TileCoord> l, WriteablePmtiles.Header header,
      long dirOffset, int dirLength) throws IOException {

      channel.position(dirOffset);
      var buf = ByteBuffer.allocate(dirLength);
      channel.read(buf);
      // TODO check compression type
      byte[] u = Gzip.gunzip(buf.array());
      var dir = WriteablePmtiles.deserializeDirectory(u);
      for (var entry : dir) {
        if (entry.runLength() == 0) {
          collectTileCoords(l, header, header.leafDirectoriesOffset() + entry.offset(), entry.length());
        } else {
          // TODO: this will only work on z15 or less pmtiles which planetiler creates
          for (int i = (int) entry.tileId(); i < entry.tileId() + entry.runLength(); i++) {
            l.add(TileCoord.hilbertDecode(i));
          }
        }
      }
    }

    private TileCoordIterator() {
      try {
        var header = getHeader();
        List<TileCoord> coords = new ArrayList<>();
        collectTileCoords(coords, header, header.rootDirOffset(), (int) header.rootDirLength());
        this.iter = coords.iterator();
      } catch (IOException e) {
        throw new IllegalStateException("Could not iterate tiles", e);
      }
    }
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    return new TileCoordIterator();
  }

  @Override
  public void close() throws IOException {
    channel.close();
  }
}
