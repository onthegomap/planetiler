package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Add missing interface from compression libraries in hadoop common. */
public interface DirectDecompressor {
  void decompress(ByteBuffer src, ByteBuffer dst) throws IOException;
}
