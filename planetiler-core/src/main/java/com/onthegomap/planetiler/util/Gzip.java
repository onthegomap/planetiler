package com.onthegomap.planetiler.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

public final class Gzip {

  private Gzip() {}

  @SuppressWarnings("java:S1168") // null in, null out
  public static byte[] gzip(byte[] in) {
    if (in == null) {
      return null;
    }
    var bos = new ByteArrayOutputStream(in.length);
    try (var gzipOS = new GZIPOutputStream(bos)) {
      gzipOS.write(in);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
    return bos.toByteArray();
  }

  @SuppressWarnings("java:S1168") // null in, null out
  public static byte[] gunzip(byte[] zipped) {
    if (zipped == null) {
      return null;
    }
    try (var is = new GZIPInputStream(new ByteArrayInputStream(zipped))) {
      return is.readAllBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static boolean isZipped(byte[] in) {
    return in != null && in.length > 2 && in[0] == (byte) GZIPInputStream.GZIP_MAGIC &&
      in[1] == (byte) (GZIPInputStream.GZIP_MAGIC >> 8);
  }
}
