package com.onthegomap.planetiler.s3;

import com.onthegomap.planetiler.archive.TileSchemeEncoding;
import com.onthegomap.planetiler.config.Arguments;
import java.net.URI;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.core.client.config.SdkAdvancedAsyncClientOption;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.S3Utilities;

class S3Options {

  private static final String OPTION_TILE_SCHEME = "tile_scheme";
  private static final String OPTION_PREFIX = "prefix";
  private static final String OPTION_DELIMITER = "delimiter";
  private static final String OPTION_REGION = "region";
  private static final String OPTION_ENDPOINT = "endpoint";
  private static final String OPTION_PROFILE_NAME = "profile_name";
  private static final String OPTION_BUCKET = "bucket";
  private static final String OPTION_EXECUTOR = "executor";
  private static final String OPTION_METADATA_KEY = "metadata_key";
  private static final String OPTION_NR_READ_PARALLEL_TILES = "nr_read_parallel_tiles";

  private final Arguments arguments;

  private final S3Uri s3Uri;

  private final String prefix;

  private final String delimiter;


  S3Options(URI uri, Arguments arguments) {
    this.arguments = arguments;
    this.s3Uri = S3Utilities.builder().region(s3Region(arguments, Region.US_EAST_1)).build()
      .parseUri(uri);
    this.prefix = arguments.getString(OPTION_PREFIX, "the prefix (path)");
    this.delimiter = arguments.getString(OPTION_DELIMITER, "the delimiter", "/");
  }

  TileSchemeEncoding createTilesSchemeEncoding() {
    final String defaultTileScheme = String.join(delimiter, TileSchemeEncoding.Z_TEMPLATE,
      TileSchemeEncoding.X_TEMPLATE, TileSchemeEncoding.Y_TEMPLATE + ".pbf");
    final String tileScheme =
      arguments.getString(OPTION_TILE_SCHEME, TileSchemeEncoding.ARGUMENT_DESCRIPTION, defaultTileScheme);
    return new TileSchemeEncoding(tileScheme, prefix, delimiter);
  }

  private static Region s3Region(Arguments args, Region defaultValue) {
    return args.getObject(OPTION_REGION, "the s3 region", defaultValue, Region::of);
  }

  String bucket() {
    return Objects.requireNonNull(arguments.getString(OPTION_BUCKET, "the bucket name", s3Uri.bucket().orElse(null)),
      "bucket must be provided");
  }

  String metadataKey() {
    return arguments.getString(OPTION_METADATA_KEY, "the (full) metadata key", prefix + delimiter + "metadata.json");
  }

  int nrReadParallelTiles() {
    final int nrReadParallelTiles = arguments.getInteger(OPTION_NR_READ_PARALLEL_TILES, "the number of tiles to read in parallel when iterating over tiles", 100);
    if (nrReadParallelTiles < 1) {
      throw new IllegalArgumentException(OPTION_NR_READ_PARALLEL_TILES + " cannot be smaller than 1");
    }
    return nrReadParallelTiles;
  }

  S3AsyncClient createS3Client() {

    final Optional<URI> endpoint =
      arguments.getObject(OPTION_ENDPOINT, "endpoint override", Optional.empty(), s -> Optional.of(URI.create(s)));
    final Optional<String> profileName =
      arguments.getObject(OPTION_PROFILE_NAME, "the profile name", Optional.empty(), Optional::of);
    final Optional<String> executorSpec =
      Optional.ofNullable(
        arguments.getString(OPTION_EXECUTOR, "the exector for the s3-client - 'fixedN' ('fixed1', 'fixed10')", null));

    final var s3Builder = S3AsyncClient.builder()
      .region(s3Region(arguments, s3Uri.region().orElse(Region.US_EAST_1)));

    endpoint.ifPresent(s3Builder::endpointOverride);
    executorSpec.map(S3Options::executorFromSpec)
      .ifPresent(es -> s3Builder.asyncConfiguration(b -> b.advancedOption(SdkAdvancedAsyncClientOption.FUTURE_COMPLETION_EXECUTOR, es)));

    final var credentialsProviderBuilder = DefaultCredentialsProvider.builder();
    profileName.ifPresent(credentialsProviderBuilder::profileName);
    s3Builder.credentialsProvider(credentialsProviderBuilder.build());

    return s3Builder.build();
  }

  private static ExecutorService executorFromSpec(String executor) {
    return switch (executor) {
      case String fixed when fixed.startsWith("fixed") -> Executors.newFixedThreadPool(Integer.parseInt(fixed.replace("fixed", "")));
      default -> throw new IllegalArgumentException("unsupported executor type");
    };
  }

}
