package com.onthegomap.planetiler.util;

import ar.com.hjg.pngj.IImageLine;
import ar.com.hjg.pngj.ImageLineInt;
import ar.com.hjg.pngj.PngReader;
import ar.com.hjg.pngj.PngWriter;
import ar.com.hjg.pngj.chunks.ChunkCopyBehaviour;
import com.carrotsearch.hppc.ByteArrayList;
import java.awt.Color;
import java.awt.Graphics;
import java.awt.image.BufferedImage;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.GZIPOutputStream;
import javax.imageio.ImageIO;

public class Terrarium {
  //scanline 11/619/746  406kb -> 944kb -> 115kb (72%)
  //scanline 11/616/743  526kb -> 1307kb -> 187kb (64%)
  //scanline 12/1238/1492  79kb -> 191kb -> 21kb (73%)
  //scanline 12/1233/1486  134kb -> 341kb -> 50kb (62%)
  //hilbert 11/619/746  406kb -> 923kb -> 124kb (69%)
  //hilbert 11/616/743  526kb -> 1362kb -> 225kb (57%)
  //hilbert 12/1238/1492  79kb -> 176kb -> 21kb (73%)
  //hilbert 12/1233/1486  134kb -> 345kb -> 58kb (56%)

  public static void main(String[] args) throws IOException {
    for (boolean b : List.of(false, true)) {
      testRGB512(11, 1238 / 2, 1492 / 2, b);
      testRGB512(11, 1233 / 2, 1486 / 2, b);
      testRGB512(5, 1 << 4, 1 << 4, b);
      testRGB(12, 1238, 1492, b);
      testRGB(12, 1233, 1486, b);
      testRGB(5, 1 << 4, 1 << 4, b);
    }
    testRGBNoBlue(12, 1238, 1492);
    testRGBNoBlue(12, 1233, 1486);
    testRGBNoBlue(5, 1 << 4, 1 << 4);
    testSkadi("https://elevation-tiles-prod.s3.amazonaws.com/v2/skadi/N44/N44W072.hgt.gz");
  }

  private static byte[] getElevation(int z, int x, int y) throws IOException {
    return new URL("https://elevation-tiles-prod.s3.amazonaws.com/terrarium/" + z + "/" + x + "/" + y + ".png")
      .openConnection().getInputStream().readAllBytes();
  }

  record Point(int x, int y) {}

  private static Point hilbert(int level, int number) {
    long xy = Hilbert.hilbertPositionToXY(level, number);
    int px = Hilbert.extractX(xy);
    int py = Hilbert.extractY(xy);
    return new Point(px, py);
  }

  private static Point scanline(int level, int number) {
    int max = 1 << level;
    return new Point(number % max, number / max);
  }

  private static void testRGB512(int z, int x, int y, boolean hilbert) throws IOException {
    var a = getElevation(z + 1, x * 2, y * 2);
    var b = getElevation(z + 1, x * 2 + 1, y * 2);
    var c = getElevation(z + 1, x * 2, y * 2 + 1);
    var d = getElevation(z + 1, x * 2 + 1, y * 2 + 1);
    BufferedImage result = new BufferedImage(
      512, 512, //work these out
      BufferedImage.TYPE_INT_RGB);
    Graphics g = result.getGraphics();
    g.drawImage(ImageIO.read(new ByteArrayInputStream(a)), 0, 0, null);
    g.drawImage(ImageIO.read(new ByteArrayInputStream(b)), 256, 0, null);
    g.drawImage(ImageIO.read(new ByteArrayInputStream(c)), 0, 256, null);
    g.drawImage(ImageIO.read(new ByteArrayInputStream(d)), 256, 256, null);
    int level = 9;
    int max = 1 << (2 * level);
    var baos = new ByteArrayOutputStream();
    int lastValue = 0;
    int lastSlope = 0;
    var bytesOut = new ByteArrayList();
    try (
      var gzos = new GZIPOutputStream(baos);
    ) {
      for (int i = 0; i < max; i++) {
        Point p = hilbert ? hilbert(level, i) : scanline(level, i);
        Color color = new Color(result.getRGB(p.x, p.y));
        double elevation = (color.getRed() * 256 + color.getGreen() + color.getBlue() / 256d) - 32768;
        int encoded = (int) Math.rint(elevation);
        int slope = encoded - lastValue;
        VarInt.putVarLong((long) slope - lastSlope, bytesOut);
        lastSlope = slope;
        lastValue = encoded;
      }
      gzos.write(bytesOut.toArray());
    }
    int before = (a.length + b.length + c.length + d.length);
    int after = baos.toByteArray().length;
    System.err
      .println(
        (hilbert ? "hilbert" : "scanline") + " " +
          z + "/" + x + "/" + y + "  " + before / 1000 + "kb -> " + bytesOut.size() / 1000 + "kb -> " + after / 1000 +
          "kb (" + Math.round(100 * (1d - (after) * 1d / before)) + "%)");
  }

  private static void testRGB(int z, int x, int y, boolean hilbert) throws IOException {
    var a = getElevation(z, x, y);
    BufferedImage result = ImageIO.read(new ByteArrayInputStream(a));
    int level = 8;
    int max = 1 << (2 * level);
    var baos = new ByteArrayOutputStream();
    int last = 0;
    var bytesOut = new ByteArrayList();
    try (
      var gzos = new GZIPOutputStream(baos);
    ) {
      for (int i = 0; i < max; i++) {
        Point p = hilbert ? hilbert(level, i) : scanline(level, i);
        Color color = new Color(result.getRGB(p.x, p.y));
        double elevation = (color.getRed() * 256 + color.getGreen() + color.getBlue() / 256d) - 32768;
        int encoded = (int) Math.rint(elevation);
        VarInt.putVarLong((long) encoded - last, bytesOut);
        last = encoded;
      }
      gzos.write(bytesOut.toArray());
    }
    int before = (a.length);
    int after = baos.toByteArray().length;
    System.err
      .println((hilbert ? "hilbert" : "scanline") + " " +
        z + "/" + x + "/" + y + "  " + before / 1000 + "kb -> " + bytesOut.size() / 1000 + "kb -> " + after / 1000 +
        "kb (" + Math.round(100 * (1d - (after) * 1d / before)) + "%)");
  }

  private static void testRGBNoBlue(int z, int x, int y) throws IOException {
    var a = getElevation(z, x, y);
    PngReader pngr = new PngReader(new ByteArrayInputStream(a));
    var baos = new ByteArrayOutputStream();
    PngWriter pngw = new PngWriter(baos, pngr.imgInfo);
    pngw.setCompLevel(-1);
    pngw.copyChunksFrom(pngr.getChunksList(), ChunkCopyBehaviour.COPY_ALL_SAFE);
    while (pngr.hasMoreRows()) {
      IImageLine l1 = pngr.readRow();
      int[] scanline = ((ImageLineInt) l1).getScanline();
      for (int j = 0; j < pngr.imgInfo.cols; j++) {
        scanline[j * 3 + 2] = 0;
      }
      pngw.writeRow(l1);
    }
    pngr.end();
    pngw.end();
    //    BufferedImage input = ImageIO.read(new ByteArrayInputStream(a));
    //    ArgbImageBuffer buffer = new ArgbImageBuffer(256, 256);
    //    for (int px = 0; px < input.getWidth(); px++) {
    //      for (int py = 0; py < input.getWidth(); py++) {
    //        Color color = new Color(input.getRGB(px, py));
    //        buffer.setRgb(px, py, color.getRed(), color.getGreen(), 0);
    //      }
    //    }
    //    var baos = new ByteArrayOutputStream();
    //
    //    try (PNGWriter writer = new PNGWriter(baos, PNGType.RGB, 9)) {
    //      writer.write(buffer);
    //    }
    //
    //
    int before = a.length;
    int after = baos.toByteArray().length;
    System.err
      .println("png-noblue " +
        z + "/" + x + "/" + y + "  " + before / 1000 + "kb -> " + after / 1000 +
        "kb (" + Math.round(100 * (1d - (after) * 1d / before)) + "%)");
  }

  private static void testSkadi(String url) throws IOException {
    System.err.println(url);
    var bytes = new URL(url).openConnection().getInputStream().readAllBytes();
    var expanded = Gzip.gunzip(bytes);

    int size = 3601;

    ByteBuffer buf = ByteBuffer.wrap(expanded);

    var baos = new ByteArrayOutputStream();
    short last = 0;
    int level = 11;
    int max = 1 << (2 * level);
    try (
      var gzos = new GZIPOutputStream(baos);
    ) {
      ByteArrayList bytesOut = new ByteArrayList();
      for (int i = 0; i < max; i++) {
        long xy = Hilbert.hilbertPositionToXY(level, i);
        int x = Hilbert.extractX(xy);
        int y = Hilbert.extractY(xy);
        short value = buf.getShort(2 * (x + y * size));
        //        Color color = new Color(img.getRGB(x, y));
        //        double elevation = (color.getRed() * 256 + color.getGreen() + color.getBlue() / 256d) - 32768;
        //        int encoded = (int) Math.rint(elevation * factor);
        VarInt.putVarLong((long) value - last, bytesOut);
        last = value;
      }
      //      for (int i = 0; i < 3601 * 3601; i++) {
      //        short item = buf.getShort();
      //        VarInt.putVarLong(item - last, bytesOut);
      //        last = item;
      //      }
      gzos.write(bytesOut.toArray());
    }

    System.err.println("  input(gz)=" + bytes.length);
    System.err.println("  input(raw)=" + expanded.length);
    System.err.println("  out=" + baos.toByteArray().length);
  }
}
