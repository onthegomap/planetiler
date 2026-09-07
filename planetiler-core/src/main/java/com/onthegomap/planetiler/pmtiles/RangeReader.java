package com.onthegomap.planetiler.pmtiles;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.Downloader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;

public interface RangeReader extends Closeable {
  byte[] read(long start, int length) throws IOException;

  class FromSeekableByteChannel implements RangeReader {

    private final SeekableByteChannel channel;

    FromSeekableByteChannel(SeekableByteChannel channel) {
      this.channel = channel;
    }

    @Override
    public synchronized byte[] read(long start, int length) throws IOException {
      var buf = ByteBuffer.allocate(length);
      channel.read(buf);
      return buf.array();
    }

    @Override
    public void close() throws IOException {
      channel.close();
    }
  }


  class FromUrl implements RangeReader {

    private final String url;
    private final PlanetilerConfig config;

    public FromUrl(String url, PlanetilerConfig config) throws MalformedURLException {
      this.url = url;
      this.config = config;
    }

    @Override
    public byte[] read(long start, int length) throws IOException {
      try (var reader = Downloader.openStreamRange(url, config, start, start + length)) {
        return reader.readAllBytes();
      }
    }

    public InputStream startReading(long start, int length) throws IOException {
      return Downloader.openStreamRange(url, config, start, start + length);
    }

    @Override
    public void close() {}
  }
}
