package com.onthegomap.planetiler.files;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

final class FilesArchiveUtils {

  static final String OPTION_METADATA_PATH = "metadata_path";
  static final String OPTION_TILE_SCHEME = "tile_scheme";

  private static final String X_TEMPLATE = "{x}";
  private static final String X_SAFE_TEMPLATE = "{xs}";
  private static final String Y_TEMPLATE = "{y}";
  private static final String Y_SAFE_TEMPLATE = "{ys}";
  private static final String Z_TEMPLATE = "{z}";

  private FilesArchiveUtils() {}

  static Optional<Path> metadataPath(Path basePath, Arguments options) {
    final String metadataPathRaw = options.getString(
      OPTION_METADATA_PATH,
      "path to the metadata - use \"none\" to disable",
      "metadata.json"
    );
    if ("none".equals(metadataPathRaw)) {
      return Optional.empty();
    } else {
      final Path p = Paths.get(metadataPathRaw);
      if (p.isAbsolute()) {
        return Optional.of(p);
      }
      return Optional.of(basePath.resolve(p));
    }
  }

  static Function<TileCoord, Path> tileSchemeEncoder(Path basePath, String validatedTileScheme) {
    final boolean xSafe = validatedTileScheme.contains(X_SAFE_TEMPLATE);
    final boolean ySafe = validatedTileScheme.contains(Y_SAFE_TEMPLATE);
    return tileCoord -> {

      String p = validatedTileScheme.replace(Z_TEMPLATE, Integer.toString(tileCoord.z()));

      if (xSafe) {
        final String xString = Integer.toString(tileCoord.x());
        var col = ("000000" + xString).substring(xString.length());
        p = p.replace(X_SAFE_TEMPLATE, Paths.get(col.substring(0, 3), col.substring(3, 6)).toString());
      } else {
        p = p.replace(X_TEMPLATE, Integer.toString(tileCoord.x()));
      }

      if (ySafe) {
        final String yString = Integer.toString(tileCoord.y());
        var row = ("000000" + yString).substring(yString.length());
        p = p.replace(Y_SAFE_TEMPLATE, Paths.get(row.substring(0, 3), row.substring(3, 6)).toString());
      } else {
        p = p.replace(Y_TEMPLATE, Integer.toString(tileCoord.y()));
      }

      return basePath.resolve(Paths.get(p));
    };
  }

  @SuppressWarnings("java:S1075")
  static Function<Path, Optional<TileCoord>> tileSchemeDecoder(Path basePath, String validatedTileScheme) {

    final String tmpPath = basePath.resolve(validatedTileScheme).toAbsolutePath().toString();

    final String escapedPathSeparator = "\\" + File.separator;

    final Pattern pathPattern = Pattern.compile(
      Pattern.quote(tmpPath)
        .replace(X_TEMPLATE, "\\E(?<x>\\d+)\\Q")
        .replace(Y_TEMPLATE, "\\E(?<y>\\d+)\\Q")
        .replace(Z_TEMPLATE, "\\E(?<z>\\d+)\\Q")
        .replace(X_SAFE_TEMPLATE, "\\E(?<x0>\\d+)" + escapedPathSeparator + "(?<x1>\\d+)\\Q")
        .replace(Y_SAFE_TEMPLATE, "\\E(?<y0>\\d+)" + escapedPathSeparator + "(?<y1>\\d+)\\Q")
    );

    final boolean xSafe = validatedTileScheme.contains(X_SAFE_TEMPLATE);
    final boolean ySafe = validatedTileScheme.contains(Y_SAFE_TEMPLATE);

    return path -> {
      final Matcher m = pathPattern.matcher(path.toAbsolutePath().toString());
      if (!m.matches()) {
        return Optional.empty();
      }
      final int x = xSafe ? Integer.parseInt(m.group("x0") + m.group("x1")) : Integer.parseInt(m.group("x"));
      final int y = ySafe ? Integer.parseInt(m.group("y0") + m.group("y1")) : Integer.parseInt(m.group("y"));
      final int z = Integer.parseInt(m.group("z"));

      return Optional.of(TileCoord.ofXYZ(x, y, z));
    };
  }

  static String tilesScheme(Arguments options) {
    final String tileScheme = options.getString(
      OPTION_TILE_SCHEME,
      "the tile scheme (e.g. {z}/{x}/{y}.pbf, {x}/{y}/{z}.pbf)" +
        " - instead of {x}/{y} {xs}/{ys} can be used which splits the x/y into 2 directories each" +
        " which ensures <1000 files per directory",
      Path.of(Z_TEMPLATE, X_TEMPLATE, Y_TEMPLATE + ".pbf").toString()
    );
    if (Paths.get(tileScheme).isAbsolute()) {
      throw new IllegalArgumentException("tile scheme is not allowed to be absolute");
    }
    if (StringUtils.countMatches(tileScheme, Z_TEMPLATE) != 1 ||
      StringUtils.countMatches(tileScheme, X_TEMPLATE) + StringUtils.countMatches(tileScheme, X_SAFE_TEMPLATE) != 1 ||
      StringUtils.countMatches(tileScheme, Y_TEMPLATE) + StringUtils.countMatches(tileScheme, Y_SAFE_TEMPLATE) != 1) {
      throw new IllegalArgumentException(
        "tile scheme must contain ('%s' OR '%s') AND ('%s' OR '%s' ) AND '%s'"
          .formatted(X_TEMPLATE, X_SAFE_TEMPLATE, Y_TEMPLATE, Y_SAFE_TEMPLATE, Z_TEMPLATE));

    }
    return tileScheme;
  }

  static int searchDepth(String validatedTileScheme) {

    return Paths.get(validatedTileScheme).getNameCount() +
      StringUtils.countMatches(validatedTileScheme, X_SAFE_TEMPLATE) +
      StringUtils.countMatches(validatedTileScheme, Y_SAFE_TEMPLATE);
  }
}
