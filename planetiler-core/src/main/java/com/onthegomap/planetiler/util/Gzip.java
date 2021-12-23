package com.onthegomap.planetiler.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public class Gzip {

  public static byte[] gzip(byte[] in) throws IOException {
    var bos = new ByteArrayOutputStream(in.length);
    try (var gzipOS = new GZIPOutputStream(bos)) {
      gzipOS.write(in);
    }
    return bos.toByteArray();
  }

  public static byte[] gunzip(byte[] zipped) throws IOException {
    try (var is = new GZIPInputStream(new ByteArrayInputStream(zipped))) {
      return is.readAllBytes();
    }
  }
}
