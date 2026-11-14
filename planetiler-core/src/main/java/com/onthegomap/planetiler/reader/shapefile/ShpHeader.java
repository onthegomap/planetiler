package com.onthegomap.planetiler.reader.shapefile;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.locationtech.jts.geom.Envelope;

/**
 * The header at the start of {@code .shp} and {@code .shx} files, from spec "The Main File Header" of the <a href=
 * "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Technical Description</a>.
 *
 * @param fileBytes   size of the whole file in bytes
 * @param shapeTypeId shape type of every record that isn't a null shape
 * @param bounds      bounding box of all records
 */
record ShpHeader(long fileBytes, int shapeTypeId, Envelope bounds) {
  // Offsets in bytes. Length is the file size in 16-bit words, and the z and m ranges are 0 if the shape type has none.

  // big endian:
  //    +------------------+----------+
  //  0 | file code (9994) | i32      |
  //  4 | unused           | 5 x i32  |
  // 24 | length           | i32      |
  //    +------------------+----------+

  // rest are little endian:
  //    +------------------+----------+
  // 28 | version (1000)   | i32      |
  // 32 | shape type       | i32      |
  // 36 | xmin             | f64      |
  // 44 | ymin             | f64      |
  // 52 | xmax             | f64      |
  // 60 | ymax             | f64      |
  // 68 | zmin             | f64      |
  // 76 | zmax             | f64      |
  // 84 | mmin             | f64      |
  // 92 | mmax             | f64      |
  //    +------------------+----------+

  static final int BYTES = 100;
  private static final int FILE_CODE = 9994;

  /** Returns whether {@code bytes} are long enough for a header and start with the shapefile file code. */
  static boolean isHeader(byte[] bytes) {
    return bytes.length >= BYTES && ByteBuffer.wrap(bytes).getInt(0) == FILE_CODE;
  }

  /** Returns the header in the first {@link #BYTES} of {@code bytes}. */
  static ShpHeader parse(byte[] bytes) {
    if (bytes.length < BYTES) {
      throw new IllegalArgumentException("A shapefile header has " + BYTES + " bytes, got " + bytes.length);
    }
    var buffer = ByteBuffer.wrap(bytes);
    long fileBytes = 2L * Integer.toUnsignedLong(buffer.getInt(24));
    buffer.order(ByteOrder.LITTLE_ENDIAN);
    double xmin = buffer.getDouble(36);
    double ymin = buffer.getDouble(44);
    double xmax = buffer.getDouble(52);
    double ymax = buffer.getDouble(60);
    var bounds = new Envelope(xmin, xmax, ymin, ymax);
    int shapeTypeId = buffer.getInt(32);
    return new ShpHeader(fileBytes, shapeTypeId, bounds);
  }

  /** Returns the number of records listed in a {@code .shx} index file of {@code indexFileBytes}. */
  static long recordsInIndexFileFromSize(long indexFileBytes) {
    // from spec "Organization of the Index File": this header, then an offset and length per record, big endian:
    //       0    1    2    3    4    5    6    7    8    9    10   11   12   13   14   15
    //     +-------------------+-------------------+-------------------+-------------------+
    // 100 |       offset      |       length      |       offset      |       length      |
    //     +-------------------+-------------------+-------------------+-------------------+
    // 116 |                                      ...                                      |
    //     +-------------------------------------------------------------------------------+
    return Math.max(0, (indexFileBytes - BYTES) / 8);
  }
}
