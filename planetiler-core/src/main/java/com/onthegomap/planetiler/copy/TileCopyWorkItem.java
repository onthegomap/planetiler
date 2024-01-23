package com.onthegomap.planetiler.copy;

import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public class TileCopyWorkItem {
  private final TileCoord coord;
  private final Supplier<byte[]> originalTileDataLoader;
  private final UnaryOperator<byte[]> reEncoder;
  private final Function<byte[], OptionalLong> hasher;
  private final CompletableFuture<byte[]> originalData = new CompletableFuture<>();
  private final CompletableFuture<byte[]> reEncodedData = new CompletableFuture<>();
  private final CompletableFuture<OptionalLong> reEncodedDataHash = new CompletableFuture<>();

  TileCopyWorkItem(
    TileCoord coord,
    Supplier<byte[]> originalTileDataLoader,
    UnaryOperator<byte[]> reEncoder,
    Function<byte[], OptionalLong> hasher
  ) {
    this.coord = coord;
    this.originalTileDataLoader = originalTileDataLoader;
    this.reEncoder = reEncoder;
    this.hasher = hasher;
  }

  public TileCoord getCoord() {
    return coord;
  }

  void loadOriginalTileData() {
    originalData.complete(originalTileDataLoader.get());
  }

  void process() throws ExecutionException, InterruptedException {
    final var reEncoded = reEncoder.apply(originalData.get());
    final var hash = hasher.apply(reEncoded);
    reEncodedData.complete(reEncoded);
    reEncodedDataHash.complete(hash);
  }

  TileEncodingResult toTileEncodingResult() throws ExecutionException, InterruptedException {
    return new TileEncodingResult(coord, reEncodedData.get(), reEncodedDataHash.get());
  }
}
