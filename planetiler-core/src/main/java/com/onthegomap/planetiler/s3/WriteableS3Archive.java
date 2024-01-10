package com.onthegomap.planetiler.s3;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.stats.Counter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

public class WriteableS3Archive implements WriteableTileArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteableS3Archive.class);

  private final Counter.MultiThreadCounter bytesWritten = Counter.newMultiThreadCounter();
  private final S3AsyncClient s3Client;
  private final String bucket;
  private final Function<TileCoord, String> tileSchemeEncoder;
  private final TileOrder tileOrder;

  private final String metadataKey;


  WriteableS3Archive(Function<S3Options, S3AsyncClient> s3ClientFactory, URI uri, Arguments formatOptions) {

    final var s3Options = new S3Options(uri, formatOptions);
    this.s3Client = s3ClientFactory.apply(s3Options);
    this.bucket = s3Options.bucket();
    final var tileSchemeEncoding = s3Options.createTilesSchemeEncoding();
    this.tileSchemeEncoder = tileSchemeEncoding.encoder();
    this.tileOrder = tileSchemeEncoding.preferredTileOrder();
    this.metadataKey = s3Options.metadataKey();
  }

  public static WriteableS3Archive newWriter(URI uri, Arguments formatOptions) {
    return new WriteableS3Archive(S3Options::createS3Client, uri, formatOptions);
  }

  @Override
  public boolean deduplicates() {
    return false;
  }

  @Override
  public TileOrder tileOrder() {
    return tileOrder;
  }

  @Override
  public TileWriter newTileWriter() {
    return new TileS3Writer(s3Client, tileSchemeEncoder, bucket, bytesWritten.counterForThread());
  }

  @Override
  public void finish(TileArchiveMetadata tileArchiveMetadata) {
    if (tileArchiveMetadata == null || metadataKey == null) {
      return;
    }
    final byte[] data;
    try {
      data = TileArchiveMetadataDeSer.mbtilesMapper().writeValueAsBytes(tileArchiveMetadata);
    } catch (JsonProcessingException e) {
      throw new UncheckedIOException(e);
    }
    final var request = PutObjectRequest.builder().bucket(bucket).key(metadataKey).build();
    try {
      s3Client.putObject(request, AsyncRequestBody.fromBytes(data)).get();
    } catch (InterruptedException ex) {
      Thread.currentThread().interrupt();
      throw new S3InteractionException(ex);
    } catch (ExecutionException ex) {
      throw new S3InteractionException(ex);
    }
  }

  @Override
  public long bytesWritten() {
    return bytesWritten.get();
  }

  @Override
  public void close() throws IOException {
    s3Client.close();
  }

  private static class TileS3Writer implements TileWriter {

    private final S3AsyncClient s3Client;
    private final Function<TileCoord, String> tileSchemeEncoder;

    private final String bucket;

    private final Counter bytesWritten;

    private final Phaser phaser = new Phaser(1); // register self

    @SuppressWarnings("java:S3077")
    private volatile S3InteractionException failure;

    TileS3Writer(S3AsyncClient s3Client, Function<TileCoord, String> tileSchemeEncoder, String bucket,
      Counter bytesWritten) {

      this.s3Client = s3Client;
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.bucket = bucket;
      this.bytesWritten = bytesWritten;
    }

    @Override
    public void write(TileEncodingResult encodingResult) {

      throwOnFailure();

      final byte[] data = encodingResult.tileData();
      final AsyncRequestBody body;
      if (data == null) {
        body = AsyncRequestBody.empty();
      } else {
        body = AsyncRequestBody.fromBytes(data);
        bytesWritten.incBy(data.length);
      }


      final String key = tileSchemeEncoder.apply(encodingResult.coord());
      final PutObjectRequest objectRequest = PutObjectRequest.builder()
        .key(key)
        .bucket(bucket)
        .build();

      phaser.register();
      s3Client.putObject(objectRequest, body).whenComplete((res, ex) -> {
        if (failure == null && ex != null) {
          failure = new S3InteractionException(ex);
          LOGGER.atError().setCause(ex).setMessage(() -> "failed to send tile" + encodingResult.coord()).log();
        }
        phaser.arriveAndDeregister();
      });
    }

    @Override
    public void close() {
      phaser.arriveAndAwaitAdvance();
    }

    private void throwOnFailure() {
      // _try_ to fail early... value may not be visible to others
      if (failure != null) {
        throw failure;
      }
    }
  }
}
