package com.onthegomap.planetiler.archive;

import static com.onthegomap.planetiler.util.LanguageUtils.nullIfEmpty;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

public class TileArchives {
  private TileArchives() {}

  public record Metadata(
    Format format,
    Scheme scheme,
    String destination,
    URI uri,
    Map<String, String> options
  ) {}

  public enum Format {
    MBTILES("mbtiles"),
    PMTILES("pmtiles");

    private final String id;

    public String id() {
      return id;
    }

    Format(String id) {
      this.id = id;
    }
  }
  public enum Scheme {
    FILE("file");

    private final String id;

    public String id() {
      return id;
    }

    Scheme(String id) {
      this.id = id;
    }
  }

  private static Scheme scheme(URI uri) {
    String scheme = uri.getScheme();
    if (scheme == null) {
      return Scheme.FILE;
    }
    for (var value : Scheme.values()) {
      if (value.id.equals(scheme)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unsupported scheme " + scheme + " from " + uri);
  }

  private static String extension(URI uri) {
    String path = uri.getPath();
    if (path != null) {
      if (path.contains(".")) {
        return nullIfEmpty(path.substring(path.lastIndexOf(".") + 1));
      }
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
          split.length == 1 ? "" : URLDecoder.decode(split[1], StandardCharsets.UTF_8)
        );
      }
    }
    return result;
  }

  private static Format format(URI uri) {
    String format = query(uri).get("format");
    if (format == null) {
      format = extension(uri);
    }
    if (format == null) {
      return Format.MBTILES;
    }
    for (var value : Format.values()) {
      if (value.id.equals(format)) {
        return value;
      }
    }
    throw new IllegalArgumentException("Unsupported format " + format + " from " + uri);
  }


  public static WriteableTileArchive newWriter(Path path, PlanetilerConfig config) throws IOException {
    return isPmtiles(path) ? WriteablePmtiles.newWriteToFile(path) :
      Mbtiles.newWriteToFileDatabase(path, config.compactDb());
  }

  private static boolean isPmtiles(Path path) {
    return FileUtils.hasExtension(path, "pmtiles");
  }

  public static ReadableTileArchive newReader(Path path, PlanetilerConfig config) throws IOException {
    return isPmtiles(path) ? ReadablePmtiles.newReadFromFile(path) : Mbtiles.newReadOnlyDatabase(path);
  }

}
