package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.util.LanguageUtils.nullIfEmpty;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.util.FileUtils;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/**
 * Definition for a tileset, parsed from a URI-like string.
 * <p>
 * {@link #from(String)} can accept:
 * <ul>
 * <li>A platform-specific absolute or relative path like {@code "./archive.mbtiles"} or
 * {@code "C:\root\archive.mbtiles"}</li>
 * <li>A URI pointing at a file, like {@code "file:///root/archive.pmtiles"} or
 * {@code "file:///C:/root/archive.pmtiles"}</li>
 * </ul>
 * <p>
 * Both of these can also have archive-specific options added to the end, for example
 * {@code "output.mbtiles?compact=false&page_size=16384"}.
 *
 * @param format  The {@link Format format} of the archive, either inferred from the filename extension or the
 *                {@code ?format=} query parameter
 * @param scheme  Scheme for accessing the archive
 * @param uri     Full URI including scheme, location, and options
 * @param options Parsed query parameters from the definition string
 */
public record TileArchiveConfig(
  Format format,
  Scheme scheme,
  URI uri,
  Map<String, String> options
) {

  private static TileArchiveConfig.Scheme getScheme(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      return Scheme.FILE;
    }
    for (var value : TileArchiveConfig.Scheme.values()) {
      if (value.id().equals(scheme)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unsupported scheme " + scheme + " from " + uri);
  }

  private static String getExtension(URI uri) {
    String path = uri.getPath();
    if (path != null && (path.contains("."))) {
      return nullIfEmpty(path.substring(path.lastIndexOf(".") + 1));
    }
    return null;
  }

  private static Map<String, String> parseQuery(URI uri) {
    String query = uri.getRawQuery();
    Map<String, String> result = new HashMap<>();
    if (query != null) {
      for (var part : query.split("&")) {
        var split = part.split("=", 2);
        result.put(
          URLDecoder.decode(split[0], StandardCharsets.UTF_8),
          split.length == 1 ? "true" : URLDecoder.decode(split[1], StandardCharsets.UTF_8)
        );
      }
    }
    return result;
  }

  private static TileArchiveConfig.Format getFormat(URI uri) {
    String format = parseQuery(uri).get("format");
    if (format == null) {
      format = getExtension(uri);
    }
    if (format == null) {
      return TileArchiveConfig.Format.MBTILES;
    }
    for (var value : TileArchiveConfig.Format.values()) {
      if (value.id().equals(format)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unsupported format " + format + " from " + uri);
  }

  /**
   * Parses a string definition of a tileset from a URI-like string.
   */
  public static TileArchiveConfig from(String string) {
    // unix paths parse fine as URIs, but need to explicitly parse windows paths with backslashes
    if (string.contains("\\")) {
      String[] parts = string.split("\\?", 2);
      string = Path.of(parts[0]).toUri().toString();
      if (parts.length > 1) {
        string += "?" + parts[1];
      }
    }
    return from(URI.create(string));
  }

  /**
   * Parses a string definition of a tileset from a URI.
   */
  public static TileArchiveConfig from(URI uri) {
    if (uri.getScheme() == null) {
      String base = Path.of(uri.getPath()).toAbsolutePath().toUri().normalize().toString();
      if (uri.getRawQuery() != null) {
        base += "?" + uri.getRawQuery();
      }
      uri = URI.create(base);
    }
    return new TileArchiveConfig(
      getFormat(uri),
      getScheme(uri),
      uri,
      parseQuery(uri)
    );
  }

  /**
   * Returns the local path on disk that this archive reads/writes to, or {@code null} if it is not on disk (ie. an HTTP
   * repository).
   */
  public Path getLocalPath() {
    return scheme == Scheme.FILE ? Path.of(URI.create(uri.toString().replaceAll("\\?.*$", ""))) : null;
  }


  /**
   * Deletes the archive if possible.
   */
  public void delete() {
    if (scheme == Scheme.FILE) {
      FileUtils.delete(getLocalPath());
    }
  }

  /**
   * Returns {@code true} if the archive already exists, {@code false} otherwise.
   */
  public boolean exists() {
    return getLocalPath() != null && Files.exists(getLocalPath());
  }

  /**
   * Returns the current size of this archive.
   */
  public long size() {
    return getLocalPath() == null ? 0 : FileUtils.size(getLocalPath());
  }

  /**
   * Returns an {@link Arguments} instance that returns the value for options directly from the query parameters in the
   * URI, or from {@code arguments} prefixed by {@code "format_"}.
   */
  public Arguments applyFallbacks(Arguments arguments) {
    return Arguments.of(options).orElse(arguments.withPrefix(format.id));
  }

  public enum Format {
    MBTILES("mbtiles",
      false /* TODO mbtiles could support append in the future by using insert statements with an "on conflict"-clause (i.e. upsert) and by creating tables only if they don't exist, yet */,
      false),
    PMTILES("pmtiles", false, false),
    CSV("csv", true, true),
    PROTO("proto", true, true),
    JSON("json", true, true);

    private final String id;
    private final boolean supportsAppend;
    private final boolean supportsConcurrentWrites;

    Format(String id, boolean supportsAppend, boolean supportsConcurrentWrites) {
      this.id = id;
      this.supportsAppend = supportsAppend;
      this.supportsConcurrentWrites = supportsConcurrentWrites;
    }

    public String id() {
      return id;
    }

    public boolean supportsAppend() {
      return supportsAppend;
    }

    public boolean supportsConcurrentWrites() {
      return supportsConcurrentWrites;
    }
  }

  public enum Scheme {
    FILE("file");

    private final String id;

    Scheme(String id) {
      this.id = id;
    }

    public String id() {
      return id;
    }
  }
}
