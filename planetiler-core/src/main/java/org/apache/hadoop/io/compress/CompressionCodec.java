package org.apache.hadoop.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Fix interface from parquet floor so we can extend it with {@link GzipCodec} and {@link Lz4Codec} */
public interface CompressionCodec {
  Decompressor createDecompressor();

  Compressor createCompressor();

  CompressionInputStream createInputStream(InputStream is, Decompressor d) throws IOException;

  CompressionOutputStream createOutputStream(OutputStream os, Compressor c) throws IOException;
}
