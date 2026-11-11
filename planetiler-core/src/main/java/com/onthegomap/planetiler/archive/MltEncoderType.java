package com.onthegomap.planetiler.archive;

import java.util.List;
import java.util.Optional;
import org.maplibre.mlt.ffi.NativeLibrary;
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
   * Loads the native library if this type may use it, and returns whether to write MLT tiles with the native encoder.
   * Falls back to Java for {@link #AUTO} where the library does not load, and logs which encoder it uses.
   *
   * @throws IllegalStateException if this is {@link #NATIVE} but the native library does not load on this platform
   */
  public boolean loadNative() {
    return loadNative(NativeLibrary::load);
  }

  /** Like {@link #loadNative()}, loading the native library with {@code loadNativeLibrary}. */
  boolean loadNative(Runnable loadNativeLibrary) {
    boolean useNative = this != JAVA && tryLoad(loadNativeLibrary);
    LOGGER.info("Using the {} MLT encoder", useNative ? "native" : "Java");
    return useNative;
  }

  private boolean tryLoad(Runnable loadNativeLibrary) {
    try {
      loadNativeLibrary.run();
      return true;
    } catch (UnsatisfiedLinkError | IllegalCallerException e) {
      if (this == NATIVE) {
        throw new IllegalStateException(describe(e) + ", use --mlt-encoder=auto or java instead", e);
      }
      LOGGER.info("{}, falling back to the Java encoder", describe(e));
      return false;
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
