package com.onthegomap.planetiler.s3;

import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import software.amazon.awssdk.core.BytesWrapper;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

public class ReadableS3Archive implements ReadableTileArchive {

  private final S3AsyncClient s3Client;
  private final String bucket;
  private final String metadataKey;
  private final Function<String, Optional<TileCoord>> tileSchemeDecoder;
  private final Function<TileCoord, String> tileSchemeEncoder;

  private final int nrReadParallelTiles;


  private ReadableS3Archive(Function<S3Options, S3AsyncClient> s3ClientFactory, URI uri, Arguments formatOptions) {

    final var s3Options = new S3Options(uri, formatOptions);
    this.s3Client = s3ClientFactory.apply(s3Options);
    this.bucket = s3Options.bucket();
    final var tileSchemeEncoding = s3Options.createTilesSchemeEncoding();
    this.tileSchemeEncoder = tileSchemeEncoding.encoder();
    this.tileSchemeDecoder = tileSchemeEncoding.decoder();
    this.metadataKey = s3Options.metadataKey();
    this.nrReadParallelTiles = s3Options.nrReadParallelTiles();
  }

  public static ReadableS3Archive newReader(URI uri, Arguments formatOptions) {
    return new ReadableS3Archive(S3Options::createS3Client, uri, formatOptions);
  }

  @Override
  public byte[] getTile(TileCoord coord) {
    return loadObjectContents(tileSchemeEncoder.apply(coord));
  }

  @Override
  public byte[] getTile(int x, int y, int z) {
    return getTile(TileCoord.ofXYZ(x, y, z));
  }


  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    @SuppressWarnings("java:S2095") final var it = new TileCoordsIterator();
    return it.map(S3ObjectWithTileCoord::coord);
  }

  @Override
  public CloseableIterator<Tile> getAllTiles() {
    @SuppressWarnings("java:S2095") final var coordsIterator = new TileCoordsIterator();
    @SuppressWarnings("java:S2095") final var tilesIterator = new TilesIterator(coordsIterator);
    return tilesIterator;
  }

  @Override
  public TileArchiveMetadata metadata() {
    if (metadataKey == null) {
      return null;
    }
    final byte[] data = loadObjectContents(metadataKey);
    if (data == null) {
      return null;
    }
    try {
      return TileArchiveMetadataDeSer.mbtilesMapper().readValue(data, TileArchiveMetadata.class);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    s3Client.close();
  }

  private byte[] loadObjectContents(String key) {
    return loadObjectsContents(Set.of(key)).get(key).orElse(null);
  }

  private Map<String, Optional<byte[]>> loadObjectsContents(Set<String> keys) {

    final Map<String, CompletableFuture<byte[]>> futures = keys.stream()
      .map(key -> GetObjectRequest.builder().bucket(bucket).key(key).build())
      .collect(Collectors.toMap(
        GetObjectRequest::key,
        r -> s3Client.getObject(r, AsyncResponseTransformer.toBytes()).thenApply(
          BytesWrapper::asByteArray))
      );

    return futures.entrySet().stream()
      .collect(Collectors.toMap(
        Map.Entry::getKey,
        e -> {
          try {
            return Optional.of(e.getValue().get());
          } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new S3InteractionException(ex);
          } catch (ExecutionException ex) {
            if (ex.getCause() instanceof NoSuchKeyException) {
              return Optional.empty();
            }
            throw new S3InteractionException(ex);
          }
        }
      ));

  }

  private class TilesIterator implements CloseableIterator<Tile> {
    private final TileCoordsIterator tileCoordsIterator;

    TilesIterator(TileCoordsIterator tileCoordsIterator) {
      this.tileCoordsIterator = tileCoordsIterator;
    }

    private final Queue<Tile> buffer = new LinkedList<>();

    @Override
    public boolean hasNext() {
      return !buffer.isEmpty() || tileCoordsIterator.hasNext();
    }

    @Override
    public Tile next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      if (!buffer.isEmpty()) {
        return buffer.poll();
      }
      buffer.addAll(loadNextTiles());
      return buffer.poll();
    }

    @Override
    public void close() {
      tileCoordsIterator.close();
    }

    private List<Tile> loadNextTiles() {

      final Map<String, TileCoord> m = new HashMap<>();

      do {
        var e = tileCoordsIterator.next();
        m.put(e.s3Object().key(), e.coord());
      } while (hasNext() && m.size() < nrReadParallelTiles);

      return loadObjectsContents(m.keySet())
        .entrySet()
        .stream()
        .map(c -> new Tile(m.get(c.getKey()), c.getValue().orElseThrow()))
        .toList();
    }
  }

  private class TileCoordsIterator implements CloseableIterator<S3ObjectWithTileCoord> {

    private static final String EMPTY_CONTINUATION_TOKEN = "init";
    private final Queue<S3ObjectWithTileCoord> buffer = new LinkedList<>();
    private String continuationToken = EMPTY_CONTINUATION_TOKEN;

    @Override
    public boolean hasNext() {
      if (!buffer.isEmpty()) {
        return true;
      }

      if (continuationToken == null) {
        return false;
      }

      // reads 1k by default
      final var request = ListObjectsV2Request.builder()
        .bucket(bucket)
        .continuationToken(EMPTY_CONTINUATION_TOKEN.equals(continuationToken) ? null : continuationToken)
        .build();

      final ListObjectsV2Response resp;
      try {
        // block here... no point in going async here
        // we want to load things in a controlled manner - and not everything into memory at once
        resp = s3Client.listObjectsV2(request).get();
      } catch (InterruptedException ex) {
        Thread.currentThread().interrupt();
        throw new S3InteractionException(ex);
      } catch (ExecutionException ex) {
        throw new S3InteractionException(ex);
      }

      continuationToken = resp.nextContinuationToken();

      final List<S3ObjectWithTileCoord> coords = resp.contents().stream()
        .map(o -> {
          final var tileCoord = tileSchemeDecoder.apply(o.key());
          return tileCoord.map(coord -> new S3ObjectWithTileCoord(o, coord));
        })
        .flatMap(Optional::stream)
        .toList();

      if (coords.isEmpty() && continuationToken != null) {
        return hasNext(); // nothing in the current batch but there's more => check that
      }

      buffer.addAll(coords);

      return !buffer.isEmpty();
    }

    @Override
    public S3ObjectWithTileCoord next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return buffer.poll();
    }

    @Override
    public void close() {
      // nothing to do here
    }
  }

  private record S3ObjectWithTileCoord(S3Object s3Object, TileCoord coord) {}

}
