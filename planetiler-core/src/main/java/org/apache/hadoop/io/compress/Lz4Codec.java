package org.apache.hadoop.io.compress;

/**
 * Make {@link io.airlift.compress.lz4.Lz4Codec} available at the location expected by
 * {@link org.apache.parquet.hadoop.metadata.CompressionCodecName} to allow deserializing parquet files that use lz4
 * compression.
 */
@SuppressWarnings("java:S2176")
public class Lz4Codec extends io.airlift.compress.lz4.Lz4Codec {}
