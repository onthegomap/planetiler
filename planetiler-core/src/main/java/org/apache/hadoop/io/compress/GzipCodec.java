package org.apache.hadoop.io.compress;

import io.airlift.compress.gzip.JdkGzipCodec;

/**
 * Make {@link JdkGzipCodec} available at the location expected by
 * {@link org.apache.parquet.hadoop.metadata.CompressionCodecName} to allow deserializing parquet files that use gzip
 * compression.
 */
public class GzipCodec extends JdkGzipCodec {}

