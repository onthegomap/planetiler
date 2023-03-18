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

public record TileArchiveConfig(
  Format format,
  Scheme scheme,
  URI uri,
  Map<String, String> options
) {

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

  private static TileArchiveConfig.Scheme scheme(URI uri) {
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

  private static String extension(URI uri) {
    String path = uri.getPath();
    if (path != null && (path.contains("."))) {
      return nullIfEmpty(path.substring(path.lastIndexOf(".") + 1));
    }
    return null;
  }

  private static Map<String, String> query(URI uri) {
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

  private static TileArchiveConfig.Format format(URI uri) {
    String format = query(uri).get("format");
    if (format == null) {
      format = extension(uri);
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

  public static TileArchiveConfig from(URI uri) {
    if (uri.getScheme() == null) {
      String base = Path.of(uri.getPath()).toAbsolutePath().toUri().normalize().toString();
      if (uri.getRawQuery() != null) {
        base += "?" + uri.getRawQuery();
      }
      uri = URI.create(base);
    }
    return new TileArchiveConfig(
      format(uri),
      scheme(uri),
      uri,
      query(uri)
    );
  }

  public Path getLocalPath() {
    return scheme == Scheme.FILE ? Path.of(URI.create(uri.toString().replaceAll("\\?.*$", ""))) : null;
  }

  public void delete() {
    if (scheme == Scheme.FILE) {
      FileUtils.delete(getLocalPath());
    }
  }

  public boolean exists() {
    return getLocalPath() != null && Files.exists(getLocalPath());
  }

  public long size() {
    return getLocalPath() == null ? 0 : FileUtils.size(getLocalPath());
  }

  public Arguments applyFallbacks(Arguments arguments) {
    return Arguments.of(options).orElse(arguments.withPrefix(format.id));
  }

  public enum Format {
    MBTILES("mbtiles"),
    PMTILES("pmtiles");

    private final String id;

    Format(String id) {
      this.id = id;
    }

    public String id() {
      return id;
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
