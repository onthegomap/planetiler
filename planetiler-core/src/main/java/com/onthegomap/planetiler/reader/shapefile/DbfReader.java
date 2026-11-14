package com.onthegomap.planetiler.reader.shapefile;

import java.io.Closeable;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads the attribute rows of a shapefile from its dBase {@code .dbf} file, one at a time.
 *
 * @see <a href="https://www.clicketyclick.dk/databases/xbase/format/dbf.html">Xbase .dbf file format</a>
 * @see <a href="https://www.clicketyclick.dk/databases/xbase/format/data_types.html">Xbase data types</a>
 */
final class DbfReader implements Closeable {
  // A .dbf file is:
  // - 32 byte header (Header)
  // - 32 byte descriptor per field (FieldDescriptor), then a 0x0D byte
  // - fixed length records: a deletion flag, then each field's value as text
  // - optional 0x1A end of file byte

  private final InputStream input;
  private final Charset charset;
  private final List<Field> fields = new ArrayList<>();
  private final long numRecords;
  private final byte[] currentRow;
  private long recordsRead = 0;

  DbfReader(InputStream input, Charset charset) throws IOException {
    this.input = input;
    this.charset = charset;
    var header = Header.parse(readFully(Header.BYTES));
    numRecords = header.numRecords();
    byte[] descriptors = readFully(header.headerBytes() - Header.BYTES);
    // the deletion flag comes before the first field
    int offsetInRecord = 1;
    Map<String, Integer> timesSeen = new HashMap<>();
    FieldDescriptor descriptor;
    for (int pos = 0; (descriptor = FieldDescriptor.parse(descriptors, pos, this.charset)) != null; pos +=
      FieldDescriptor.BYTES) {
      String name = descriptor.name();
      // Writers truncate field names to 10 characters, so a name can repeat. Number the repeats (NAME, NAME1, NAME2)
      // to keep every value, matching GeoTools.
      int earlier = timesSeen.merge(name, 1, Integer::sum) - 1;
      if (earlier > 0) {
        name += earlier;
      }
      fields.add(new Field(name, descriptor.type(), offsetInRecord, descriptor.length(), descriptor.decimals()));
      offsetInRecord += descriptor.length();
    }
    currentRow = new byte[Math.max(header.recordBytes(), offsetInRecord)];
  }

  List<String> fieldNames() {
    return fields.stream().map(Field::name).toList();
  }

  /** Returns the next row, or {@code null} after the last one. */
  Row next() throws IOException {
    // each record is rlen bytes, with '*' in del if deleted:
    //     0    1    2    3    4    5    6    7    8    9    10   11   12   13   14   15
    //   +----+----------------------------------+---------------------------------------+
    // 0 |del |   field 1 as text (len bytes)    |              field 2 ...              |
    //   +----+----------------------------------+---------------------------------------+
    if (recordsRead >= numRecords || input.readNBytes(currentRow, 0, currentRow.length) < currentRow.length) {
      return null;
    }
    recordsRead++;
    Object[] values = new Object[fields.size()];
    for (int i = 0; i < values.length; i++) {
      values[i] = parse(fields.get(i));
    }
    return new Row(currentRow[0] == '*', values);
  }

  private Object parse(Field field) {
    int start = field.offsetInRecord;
    int end = start + field.length;
    return switch (field.type) {
      case 'C' -> parseText(start, end);
      case 'N', 'F' -> parseNumber(field, start, end);
      case 'L' -> parseLogical(start, end);
      case 'D' -> parseDate(start, end);
      default -> new String(currentRow, start, field.length, charset).strip();
    };
  }

  private String parseText(int start, int end) {
    boolean allZero = true;
    for (int i = start; i < end && allZero; i++) {
      allZero = currentRow[i] == 0;
    }
    if (allZero) {
      return null;
    }
    // From spec "Data types": text is padded with spaces. Some writers pad with null bytes instead, and values
    // sometimes start or end with other whitespace.
    return new String(currentRow, start, end - start, charset).trim();
  }

  private Object parseNumber(Field field, int start, int end) {
    // From spec "Data types": numbers are ASCII text, so decode them the same way whatever the .cpg charset is.
    // ISO-8859-1 maps each byte to one character and never fails.
    String text = new String(currentRow, start, end - start, StandardCharsets.ISO_8859_1).trim();
    // blank or overflowed (******) numbers have no value
    if (text.isEmpty() || text.charAt(0) == '*') {
      return null;
    }
    try {
      if (field.type == 'F' || field.decimals > 0) {
        return Double.parseDouble(text);
      } else if (field.length < 10) {
        return Integer.parseInt(text);
      } else {
        return Long.parseLong(text);
      }
    } catch (NumberFormatException _) {
      // Read numbers that don't fit the declared type (decimals or exponents in a field without decimals, integers too
      // big for a long) as doubles, matching legacy GeoTools behavior.
      try {
        return Double.parseDouble(text);
      } catch (NumberFormatException _) {
        return null;
      }
    }
  }

  private Boolean parseLogical(int start, int end) {
    // From spec "Data types": ? (not set), Y / y, N / n, T / t or F / f. Anything else, like a space, has no value.
    return switch (start < end ? Character.toUpperCase((char) currentRow[start]) : '?') {
      case 'T', 'Y' -> Boolean.TRUE;
      case 'F', 'N' -> Boolean.FALSE;
      default -> null;
    };
  }

  private Date parseDate(int start, int end) {
    // From spec "Data types": dates are ASCII digits YYYYMMDD, so like numbers decode them with ISO-8859-1 whatever the
    // .cpg charset is. Blank and 00000000 dates have no value, matching legacy GeoTools behavior. Invalid dates like
    // 20240230 have no value either, where GeoTools rolled them over to the next valid date.
    String text = new String(currentRow, start, end - start, StandardCharsets.ISO_8859_1).trim();
    if (text.length() != 8 || text.equals("00000000")) {
      return null;
    }
    try {
      // Midnight in the default time zone to match legacy GeoTools behavior. java.time, not Calendar.getInstance(),
      // whose calendar depends on the locale (Buddhist years in Thailand).
      // TODO switch to LocalDate so the value (and tiles that include it) don't depend on the machine's time zone
      int year = Integer.parseInt(text, 0, 4, 10);
      int month = Integer.parseInt(text, 4, 6, 10);
      int day = Integer.parseInt(text, 6, 8, 10);
      var date = LocalDate.of(year, month, day);
      return Date.from(date.atStartOfDay(ZoneId.systemDefault()).toInstant());
    } catch (NumberFormatException | DateTimeException _) {
      return null;
    }
  }

  private byte[] readFully(int length) throws IOException {
    byte[] result = input.readNBytes(Math.max(length, 0));
    if (result.length < length) {
      throw new EOFException("dbf file ends in its header");
    }
    return result;
  }

  @Override
  public void close() throws IOException {
    input.close();
  }

  private record Field(String name, char type, int offsetInRecord, int length, int decimals) {}

  /** A row of attribute values, in field order, and whether it is marked deleted. */
  record Row(boolean deleted, Object[] values) {
    @Override
    public boolean equals(Object o) {
      return o instanceof Row(boolean otherDeleted, Object[] otherValues) && deleted == otherDeleted &&
        Arrays.equals(values, otherValues);
    }

    @Override
    public int hashCode() {
      return 31 * Boolean.hashCode(deleted) + Arrays.hashCode(values);
    }

    @Override
    public String toString() {
      return "Row[deleted=" + deleted + ", values=" + Arrays.toString(values) + "]";
    }
  }

  /**
   * The header at the start of a dBase {@code .dbf} file.
   *
   * @param numRecords  number of records in the file
   * @param headerBytes size of this header and the field descriptors after it, so where the first record starts
   * @param recordBytes size of each record
   */
  private record Header(long numRecords, int headerBytes, int recordBytes) {
    // dBase III header, little endian:
    //      0    1    2    3    4    5    6    7    8    9    10   11   12   13   14   15
    //    +----+--------------+-------------------+---------+---------+-------------------+
    //  0 |ver |     date     |      records      |   hlen  |   rlen  |      reserved     |
    //    +----+--------------+-------------------+---------+---------+-------------------+
    // 16 |                                    reserved                                   |
    //    +-------------------------------------------------------------------------------+

    static final int BYTES = 32;

    /** Returns the header in the first {@link #BYTES} of {@code bytes}. */
    static Header parse(byte[] bytes) {
      if (bytes.length < BYTES) {
        throw new IllegalArgumentException("A dbf header has " + BYTES + " bytes, got " + bytes.length);
      }
      var buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      long numRecords = Integer.toUnsignedLong(buffer.getInt(4));
      int headerBytes = Short.toUnsignedInt(buffer.getShort(8));
      int recordBytes = Short.toUnsignedInt(buffer.getShort(10));
      return new Header(numRecords, headerBytes, recordBytes);
    }
  }

  /**
   * A field descriptor from the header of a dBase {@code .dbf} file.
   *
   * @param name     field name, at most 11 bytes
   * @param type     field type in upper case, like {@code C} for text or {@code N} for numbers
   * @param length   size of the field's value in each record
   * @param decimals digits after the decimal point in numbers
   */
  private record FieldDescriptor(String name, char type, int length, int decimals) {
    // One 32 byte descriptor per field, ending with a 0x0D byte:
    //      0    1    2    3    4    5    6    7    8    9    10   11   12   13   14   15
    //    +------------------------------------------------------+----+-------------------+
    //  0 |                         name                         |type|      reserved     |
    //    +----+----+--------------------------------------------+----+-------------------+
    // 16 |len |dec |                               reserved                              |
    //    +----+----+---------------------------------------------------------------------+

    static final int BYTES = 32;

    /**
     * Returns the descriptor at {@code offset} in {@code descriptors}, or {@code null} if the descriptors end there.
     */
    static FieldDescriptor parse(byte[] descriptors, int offset, Charset charset) {
      if (offset + BYTES > descriptors.length || descriptors[offset] == 0x0D) {
        return null;
      }
      int nameLength = 0;
      while (nameLength < 11 && descriptors[offset + nameLength] != 0) {
        nameLength++;
      }
      String name = new String(descriptors, offset, nameLength, charset).trim();
      char type = Character.toUpperCase((char) descriptors[offset + 11]);
      int length = Byte.toUnsignedInt(descriptors[offset + 16]);
      int decimals = Byte.toUnsignedInt(descriptors[offset + 17]);
      return new FieldDescriptor(name, type, length, decimals);
    }
  }
}
