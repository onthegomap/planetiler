package com.onthegomap.planetiler.stream;

import com.google.common.net.UrlEscapers;
import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.commons.text.StringEscapeUtils;

public final class StreamArchiveUtils {

  private static final Pattern quotedPattern = Pattern.compile("^'(.+?)'$");

  private StreamArchiveUtils() {}

  public static Path constructIndexedPath(Path basePath, int index) {
    return index == 0 ? basePath : Paths.get(basePath.toString() + index);
  }

  static String getEscapedString(Arguments options, TileArchiveConfig.Format format, String key,
    String descriptionPrefix, String defaultValue, List<String> examples) {

    final String cliKey = format.id() + "_" + key;

    final String fullDescription = descriptionPrefix +
      " - pass it as option: " +
      examples.stream().map(e -> "%s=%s".formatted(cliKey, escapeJava(e))).collect(Collectors.joining(" | ")) +
      ", or append to the file: " +
      examples.stream().map(e -> "?%s=%s".formatted(key, escapeJavaUri(e))).collect(Collectors.joining(" | "));

    final String rawOptionValue = options.getString(key, fullDescription, defaultValue);
    return quotedPattern.matcher(rawOptionValue)
      // allow values to be wrapped by single quotes => allows to pass a space which otherwise gets trimmed
      .replaceAll("$1")
      // \n -> newline...
      .translateEscapes();
  }

  private static String escapeJava(String s) {
    if (!s.trim().equals(s)) {
      s = "'" + s + "'";
    }
    return StringEscapeUtils.escapeJava(s);
  }

  private static String escapeJavaUri(String s) {
    return UrlEscapers.urlFormParameterEscaper().escape(escapeJava(s));
  }
}
