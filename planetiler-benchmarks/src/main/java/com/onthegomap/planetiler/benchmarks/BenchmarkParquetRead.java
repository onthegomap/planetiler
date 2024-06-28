package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.reader.parquet.ParquetInputFile;
import java.nio.file.Path;

public class BenchmarkParquetRead {

  public static void main(String[] args) {
    var arguments = Arguments.fromArgs(args);
    var path =
      arguments.inputFile("parquet", "parquet file to read", Path.of("data", "sources", "locality.zstd.parquet"));
    long c = 0;
    var file = new ParquetInputFile("parquet", "locality", path, null, Bounds.WORLD, null, tags -> tags.get("id"));
    for (int i = 0; i < 20; i++) {
      long start = System.currentTimeMillis();
      for (var block : file.get()) {
        for (var item : block) {
          c += item.tags().size();
        }
      }
      System.err.println(System.currentTimeMillis() - start);
    }

    System.err.println(c);
  }
}
