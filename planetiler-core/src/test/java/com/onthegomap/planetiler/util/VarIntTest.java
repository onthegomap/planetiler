package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.Test;

class VarIntTest {

  @Test
  void testRoundTrip() throws IOException {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    VarInt.putVarLong(0, stream);
    VarInt.putVarLong(1, stream);
    VarInt.putVarLong(Long.MAX_VALUE, stream);
    VarInt.putVarLong(Long.MIN_VALUE, stream);
    ByteBuffer output = ByteBuffer.wrap(stream.toByteArray());
    assertEquals(0, VarInt.getVarLong(output));
    assertEquals(1, VarInt.getVarLong(output));
    assertEquals(Long.MAX_VALUE, VarInt.getVarLong(output));
    assertEquals(Long.MIN_VALUE, VarInt.getVarLong(output));
  }

  @Test
  void testUnsignedEncoding() throws IOException {
    byte[] rawbytes = {0, 1, 127, (byte) 0xe5, (byte) 0x8e, (byte) 0x26};
    ByteBuffer buf = ByteBuffer.wrap(rawbytes);

    assertEquals(0, VarInt.getVarLong(buf));
    assertEquals(1, VarInt.getVarLong(buf));
    assertEquals(127, VarInt.getVarLong(buf));
    assertEquals(624485, VarInt.getVarLong(buf));

    byte[] max_safe_js_integer =
      {(byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, (byte) 0xff, 0xf};
    assertEquals(9007199254740991L, VarInt.getVarLong(ByteBuffer.wrap(max_safe_js_integer)));
  }
}
