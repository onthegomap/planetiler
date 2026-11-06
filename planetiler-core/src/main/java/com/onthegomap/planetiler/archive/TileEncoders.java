package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.util.Optional;
import java.util.function.Supplier;
import org.maplibre.mlt.ffi.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the {@link TileEncoder} for the tile format and encoder settings of a {@link PlanetilerConfig}. The native
 * MLT encoder needs the JVM option {@code --enable-native-access=ALL-UNNAMED}, which planetiler's jar and image set.
 */
final class TileEncoders {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileEncoders.class);

  private TileEncoders() {}

  /**
   * Returns a factory of encoders for the tile format of {@code config}, to call once per encoding thread.
   *
   * @throws IllegalStateException if {@code mlt_encoder=native} but the native library does not load on this platform
   */
  static Supplier<TileEncoder> factory(PlanetilerConfig config, Stats stats) {
    return factory(config, stats, NativeLibrary::load);
  }

  /** Like {@link #factory(PlanetilerConfig, Stats)}, loading the native library with {@code loadNativeLibrary}. */
  static Supplier<TileEncoder> factory(PlanetilerConfig config, Stats stats, Runnable loadNativeLibrary) {
    return switch (config.tileFormat()) {
      case MLT -> mltFactory(config, stats, loadNativeLibrary);
      case UNKNOWN, MVT -> MvtTileEncoder::new;
    };
  }

  /**
   * Returns a factory of the MLT encoder that {@code mlt_encoder} picks, falling back to Java for {@code auto} where
   * the native library does not load, and logs which one it uses.
   */
  private static Supplier<TileEncoder> mltFactory(PlanetilerConfig config, Stats stats, Runnable loadNativeLibrary) {
    boolean useNative = switch (config.mltEncoder()) {
      case JAVA -> false;
      case NATIVE -> {
        var failure = loadFailure(loadNativeLibrary);
        if (failure.isPresent()) {
          throw new IllegalStateException(describe(failure.get()) + ", use --mlt-encoder=auto or java instead",
            failure.get());
        }
        yield true;
      }
      case AUTO -> {
        var failure = loadFailure(loadNativeLibrary);
        failure.ifPresent(e -> LOGGER.info("{}, falling back to the Java encoder", describe(e)));
        yield failure.isEmpty();
      }
    };
    LOGGER.info("Using the {} MLT encoder", useNative ? "native" : "Java");
    return useNative ? () -> new NativeMltTileEncoder(config, stats) : () -> new JavaMltTileEncoder(config, stats);
  }

  /** Returns why {@code loadNativeLibrary} failed, if it failed because the native library cannot be used here. */
  private static Optional<Throwable> loadFailure(Runnable loadNativeLibrary) {
    try {
      loadNativeLibrary.run();
      return Optional.empty();
    } catch (UnsatisfiedLinkError | IllegalCallerException e) {
      return Optional.of(e);
    }
  }

  private static String describe(Throwable loadFailure) {
    return switch (loadFailure) {
      case IllegalCallerException _ -> "Native MLT encoder needs the JVM option --enable-native-access=ALL-UNNAMED";
      default -> "Native MLT encoder does not load on " + System.getProperty("os.name") + " " +
        System.getProperty("os.arch") + " (not a planetiler bug, ask MapLibre to support this platform): " +
        loadFailure.getMessage();
    };
  }
}
