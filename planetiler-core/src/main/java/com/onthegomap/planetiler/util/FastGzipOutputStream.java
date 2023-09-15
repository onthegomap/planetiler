package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

/**
 * A version of {@link GZIPOutputStream} that uses {@link Deflater#BEST_SPEED} (level 1) instead of
 * {@link Deflater#DEFAULT_COMPRESSION} (-1).
 */
public class FastGzipOutputStream extends GZIPOutputStream {

  public FastGzipOutputStream(OutputStream out) throws IOException {
    super(out);
    def.setLevel(Deflater.BEST_SPEED);
  }
}
