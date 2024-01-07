package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

/**
 * Tile scheme encoding i.e. encoding and decoding of tile coordinates to a relative path.
 * <p/>
 * The tile scheme is a template string that supports the following templates: {x}, {y}, {z}, {xs}, {ys}. {xs} and {ys}
 * are "safe" options that split the x/s coordinate into two folders, ensuring that each folder has less than 1000
 * children.
 * <table>
 * <tr>
 * <th>Tile Scheme</th>
 * <th>Example Path</th>
 * </tr>
 * <tr>
 * <td>{z}/{x}/{y}.pbf</td>
 * <td>3/1/2.pbf</td>
 * </tr>
 * <tr>
 * <td>{x}/{y}/{z}.pbf</td>
 * <td>1/2/3.pbf</td>
 * </tr>
 * <tr>
 * <td>{x}-{y}-{z}.pbf</td>
 * <td>1-2-3.pbf</td>
 * </tr>
 * <tr>
 * <td>{x}/a/{y}/b{z}.pbf</td>
 * <td>1/a/2/b3.pbf</td>
 * </tr>
 * <tr>
 * <td>{z}/{x}/{y}.pbf.gz</td>
 * <td>3/1/2.pbf.gz</td>
 * </tr>
 * <tr>
 * <td>{z}/{xs}/{ys}.pbf</td>
 * <td>3/000/001/000/002.pbf</td>
 * </tr>
 * <tr>
 * <td>{z}/{x}/{ys}.pbf</td>
 * <td>3/1/000/002.pbf</td>
 * </tr>
 * <tr>
 * <td>{z}/{xs}/{y}.pbf</td>
 * <td>3/000/001/2.pbf</td>
 * </tr>
 * </table>
 */
public class TileSchemeEncoding {

  public static final String X_TEMPLATE = "{x}";
  public static final String X_SAFE_TEMPLATE = "{xs}";
  public static final String Y_TEMPLATE = "{y}";
  public static final String Y_SAFE_TEMPLATE = "{ys}";
  public static final String Z_TEMPLATE = "{z}";

  public static final String ARGUMENT_DESCRIPTION = "the tile scheme (e.g. {z}/{x}/{y}.pbf, {x}/{y}/{z}.pbf)" +
    " - instead of {x}/{y} {xs}/{ys} can be used which splits the x/y into 2 directories each" +
    " which ensures <1000 files per directory";

  private final String tileScheme;
  private final String prefix;
  private final String delimiter;

  /**
   * @param tileScheme the tile scheme to use e.g. {z}/{x}/{y}.pbf
   * @param basePath   the base path to append the generated relative tile path to
   */
  public TileSchemeEncoding(String tileScheme, Path basePath) {
    this(checkTileSchemePathNotAbsolute(tileScheme), basePath.toAbsolutePath().toString(), File.separator);
  }

  public TileSchemeEncoding(String tileScheme, String prefix, String delimiter) {
    this.tileScheme = validate(tileScheme);
    this.prefix = prefix;
    this.delimiter = delimiter;
  }

  public Function<TileCoord, String> encoder() {
    final boolean xSafe = tileScheme.contains(X_SAFE_TEMPLATE);
    final boolean ySafe = tileScheme.contains(Y_SAFE_TEMPLATE);
    return tileCoord -> {

      String p = tileScheme.replace(Z_TEMPLATE, Integer.toString(tileCoord.z()));

      if (xSafe) {
        final String colStr = String.format("%06d", tileCoord.x());
        p = p.replace(X_SAFE_TEMPLATE, String.join(delimiter, colStr.substring(0, 3), colStr.substring(3)));
      } else {
        p = p.replace(X_TEMPLATE, Integer.toString(tileCoord.x()));
      }

      if (ySafe) {
        final String rowStr = String.format("%06d", tileCoord.y());
        p = p.replace(Y_SAFE_TEMPLATE, String.join(delimiter, rowStr.substring(0, 3), rowStr.substring(3)));
      } else {
        p = p.replace(Y_TEMPLATE, Integer.toString(tileCoord.y()));
      }
      return appendToPrefix(p);
    };
  }

  public Function<String, Optional<TileCoord>> decoder() {

    final String tmpPath = appendToPrefix(tileScheme);

    @SuppressWarnings("java:S1075") final String escapedPathSeparator = "\\" + File.separator;

    final Pattern pathPattern = Pattern.compile(
      Pattern.quote(tmpPath)
        .replace(X_TEMPLATE, "\\E(?<x>\\d+)\\Q")
        .replace(Y_TEMPLATE, "\\E(?<y>\\d+)\\Q")
        .replace(Z_TEMPLATE, "\\E(?<z>\\d+)\\Q")
        .replace(X_SAFE_TEMPLATE, "\\E(?<x0>\\d+)" + escapedPathSeparator + "(?<x1>\\d+)\\Q")
        .replace(Y_SAFE_TEMPLATE, "\\E(?<y0>\\d+)" + escapedPathSeparator + "(?<y1>\\d+)\\Q")
    );

    final boolean xSafe = tileScheme.contains(X_SAFE_TEMPLATE);
    final boolean ySafe = tileScheme.contains(Y_SAFE_TEMPLATE);

    return key -> {
      final Matcher m = pathPattern.matcher(key);
      if (!m.matches()) {
        return Optional.empty();
      }
      final int x = xSafe ? Integer.parseInt(m.group("x0") + m.group("x1")) : Integer.parseInt(m.group("x"));
      final int y = ySafe ? Integer.parseInt(m.group("y0") + m.group("y1")) : Integer.parseInt(m.group("y"));
      final int z = Integer.parseInt(m.group("z"));

      return Optional.of(TileCoord.ofXYZ(x, y, z));
    };
  }

  public int searchDepth() {
    return tileScheme.split(Pattern.quote(delimiter)).length +
      StringUtils.countMatches(tileScheme, X_SAFE_TEMPLATE) +
      StringUtils.countMatches(tileScheme, Y_SAFE_TEMPLATE);
  }

  public TileOrder preferredTileOrder() {
    // there's only TMS currently - but once there are more, this can be changed according to the scheme
    return TileOrder.TMS;
  }

  private String appendToPrefix(String s) {
    return "".equals(prefix) ? s : prefix + delimiter + s;
  }

  private static String validate(String tileScheme) {
    if (tileScheme.startsWith("/")) {
      throw new IllegalArgumentException("tile scheme is not allowed to be absolute");
    }
    if (StringUtils.countMatches(tileScheme, Z_TEMPLATE) != 1 ||
      StringUtils.countMatches(tileScheme, X_TEMPLATE) + StringUtils.countMatches(tileScheme, X_SAFE_TEMPLATE) != 1 ||
      StringUtils.countMatches(tileScheme, Y_TEMPLATE) + StringUtils.countMatches(tileScheme, Y_SAFE_TEMPLATE) != 1) {
      throw new IllegalArgumentException(
        "tile scheme must contain ('%s' OR '%s') AND ('%s' OR '%s' ) AND '%s'"
          .formatted(X_TEMPLATE, X_SAFE_TEMPLATE, Y_TEMPLATE, Y_SAFE_TEMPLATE, Z_TEMPLATE));

    }
    if (tileScheme.contains("\\E") || tileScheme.contains("\\Q")) {
      throw new IllegalArgumentException("regex quotes are not allowed");
    }
    return tileScheme;
  }

  private static String checkTileSchemePathNotAbsolute(String tileScheme) {
    if (Paths.get(tileScheme).isAbsolute()) {
      throw new IllegalArgumentException("tile scheme is not allowed to be absolute");
    }
    return tileScheme;
  }

  @Override
  public boolean equals(Object o) {
    return this == o || (o instanceof TileSchemeEncoding that && Objects.equals(tileScheme, that.tileScheme) &&
      Objects.equals(prefix, that.prefix) && Objects.equals(delimiter, that.delimiter));
  }

  @Override
  public int hashCode() {
    return Objects.hash(tileScheme, prefix, delimiter);
  }

  @Override
  public String toString() {
    return "TileSchemeEncoding[" +
      "tileScheme='" + tileScheme + '\'' +
      ", prefix=" + prefix +
      ", delimiter=" + delimiter +
      ']';
  }
}
