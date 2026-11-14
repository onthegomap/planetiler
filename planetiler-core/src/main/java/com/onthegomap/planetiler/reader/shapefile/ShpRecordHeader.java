package com.onthegomap.planetiler.reader.shapefile;

import java.nio.ByteBuffer;

/**
 * The header before the contents of each record in a {@code .shp} file, from spec "Record Headers" of the <a href=
 * "https://www.esri.com/content/dam/esrisites/sitecore-archive/Files/Pdfs/library/whitepapers/pdfs/shapefile.pdf">ESRI
 * Shapefile Technical Description</a>.
 *
 * @param recordNumber number of the record, starting at 1
 * @param contentBytes size of the contents that follow in bytes
 */
record ShpRecordHeader(int recordNumber, int contentBytes) {
  // Big endian, and length is the content size in 16-bit words:
  //     0    1    2    3    4    5    6    7    8    9    10   11   12   13   14   15
  //   +-------------------+-------------------+
  // 0 |       number      |       length      |
  //   +-------------------+-------------------+---------------------------------------+
  // 8 |                           contents: 2 x length bytes                          |
  //   +-------------------------------------------------------------------------------+

  static final int BYTES = 8;

  /** Returns the record header in the first {@link #BYTES} of {@code bytes}. */
  static ShpRecordHeader parse(byte[] bytes) {
    if (bytes.length < BYTES) {
      throw new IllegalArgumentException("A record header has " + BYTES + " bytes, got " + bytes.length);
    }
    int recordNumber = ByteBuffer.wrap(bytes).getInt(0);
    int contentBytes = parseContentLength(bytes);
    return new ShpRecordHeader(recordNumber, contentBytes);
  }

  /** Returns {@link #contentBytes()} from the first {@link #BYTES} of {@code bytes} without reading the rest. */
  static int parseContentLength(byte[] bytes) {
    return 2 * ByteBuffer.wrap(bytes).getInt(4);
  }
}
