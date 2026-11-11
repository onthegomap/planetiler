package com.onthegomap.planetiler.archive;

import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Which encoder {@link TileEncoder#factory} uses to write MLT tiles, set by {@code mlt_encoder}. */
public enum MltEncoderType {
  /** The native encoder where its library loads, and the Java encoder everywhere else. */
  AUTO("auto"),
  /** The native (Rust) encoder of {@code mlt-ffi-java}, failing on platforms where its library does not load. */
  NATIVE("native"),
  /** The pure-Java encoder of {@code org.maplibre:mlt}, which works on every platform. */
  JAVA("java");

  private static final Logger LOGGER = LoggerFactory.getLogger(MltEncoderType.class);

  private final String id;

  MltEncoderType(String id) {
    this.id = id;
  }

  public static MltEncoderType fromId(String id) {
    return findById(id)
      .orElseThrow(() -> new IllegalArgumentException("invalid MLT encoder ID " + id + "; expected one of " +
        availableValues().stream().map(MltEncoderType::id).toList()));
  }

  public static Optional<MltEncoderType> findById(String id) {
    return availableValues().stream().filter(type -> type.id.equals(id)).findFirst();
  }

  public static List<MltEncoderType> availableValues() {
    return List.of(values());
  }

  public String id() {
    return id;
  }

  /**
   * Returns whether to write MLT tiles with the native encoder, falling back to Java for {@link #AUTO} where
   * {@code loadNativeLibrary} fails, and logs which one it uses.
   *
   * @throws IllegalStateException if this is {@link #NATIVE} but the native library does not load on this platform
   */
  boolean usesNative(Runnable loadNativeLibrary) {
    boolean useNative = switch (this) {
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
    return useNative;
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
