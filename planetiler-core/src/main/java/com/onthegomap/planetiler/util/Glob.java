package com.onthegomap.planetiler.util;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;


/**
 * Utility for constructing base+glob paths for matching many files
 */
public record Glob(Path base, String pattern) {

  private static final Pattern GLOB_PATTERN = Pattern.compile("[?*{\\[].*$");

  /** Wrap a base path with no globs in it yet. */
  public static Glob of(Path path) {
    return new Glob(path, null);
  }

  /** Resolves a subdirectory using parts separated by the platform file separator. */
  public Glob resolve(String... subPath) {
    String separator = base.getFileSystem().getSeparator();
    if (pattern != null) {
      return new Glob(base, pattern + separator + String.join(separator, subPath));
    } else if (subPath == null || subPath.length == 0) {
      return this;
    } else if (GLOB_PATTERN.matcher(subPath[0]).find()) {
      return new Glob(base, String.join(separator, subPath));
    } else {
      return of(base.resolve(subPath[0])).resolve(Arrays.copyOfRange(subPath, 1, subPath.length));
    }
  }

  /** Parse a string containing platform-specific file separators into a base+glob pattern. */
  public static Glob parse(String path) {
    var matcher = GLOB_PATTERN.matcher(path);
    if (!matcher.find()) {
      return of(Path.of(path));
    }
    matcher.reset();
    String base = matcher.replaceAll("");
    String separator = Path.of(base).getFileSystem().getSeparator();
    int idx = base.lastIndexOf(separator);
    if (idx > 0) {
      base = base.substring(0, idx);
    }
    return of(Path.of(base)).resolve(path.substring(idx + 1).split(Pattern.quote(separator)));
  }

  /** Search the filesystem for all files beneath {@link #base()} matching {@link #pattern()}. */
  public List<Path> find() {
    return pattern == null ? List.of(base) : FileUtils.walkPathWithPattern(base, pattern);
  }
}
