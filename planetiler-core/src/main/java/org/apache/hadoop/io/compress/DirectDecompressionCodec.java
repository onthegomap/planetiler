package org.apache.hadoop.io.compress;

/** Add missing interface from compression libraries in hadoop common. */
public interface DirectDecompressionCodec extends CompressionCodec {
  DirectDecompressor createDirectDecompressor();
}
