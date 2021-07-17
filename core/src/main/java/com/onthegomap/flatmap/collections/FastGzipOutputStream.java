package com.onthegomap.flatmap.collections;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.Deflater;
import java.util.zip.GZIPOutputStream;

public class FastGzipOutputStream extends GZIPOutputStream {

  public FastGzipOutputStream(OutputStream out) throws IOException {
    super(out);
    def.setLevel(Deflater.BEST_SPEED);
  }
}
