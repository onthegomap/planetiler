package com.onthegomap.planetiler.stream;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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

  /**
   * exposing meta data (non-tile data) might be useful for most use cases but complicates parsing for simple use cases
   * => allow to output tiles, only
   */
  private static final String JSON_OPTION_WRITE_TILES_ONLY = "tiles_only";

  private static final String JSON_OPTION_ROOT_VALUE_SEPARATOR = "root_value_separator";

  static final String CSV_OPTION_COLUMN_SEPARATOR = "column_separator";
  static final String CSV_OPTION_LINE_SEPARATOR = "line_separator";
  static final String CSV_OPTION_BINARY_ENCODING = "binary_encoding";

  private static final Pattern quotedPattern = Pattern.compile("^'(.+?)'$");

  static final JsonMapper jsonMapperJsonStreamArchive = JsonMapper.builder()
    .serializationInclusion(JsonInclude.Include.NON_ABSENT)
    .addModule(new Jdk8Module())
    .build();

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

  static String jsonOptionRootValueSeparator(Arguments formatOptions) {
    return getEscapedString(formatOptions, TileArchiveConfig.Format.JSON,
      JSON_OPTION_ROOT_VALUE_SEPARATOR, "root value separator", "'\\n'", List.of("\n", " "));
  }

  static boolean jsonOptionWriteTilesOnly(Arguments formatOptions) {
    return formatOptions.getBoolean(JSON_OPTION_WRITE_TILES_ONLY, "write tiles, only", false);
  }

  static String csvOptionColumnSeparator(Arguments formatOptions, TileArchiveConfig.Format format) {
    final String defaultColumnSeparator = switch (format) {
      case CSV -> "','";
      case TSV -> "'\\t'";
      default -> throw new IllegalArgumentException("supported formats are csv and tsv but got " + format.id());
    };
    return getEscapedString(formatOptions, format,
      CSV_OPTION_COLUMN_SEPARATOR, "column separator", defaultColumnSeparator, List.of(",", " "));
  }

  static String csvOptionLineSeparator(Arguments formatOptions, TileArchiveConfig.Format format) {
    return StreamArchiveUtils.getEscapedString(formatOptions, format,
      CSV_OPTION_LINE_SEPARATOR, "line separator", "'\\n'", List.of("\n", "\r\n"));
  }

  static CsvBinaryEncoding csvOptionBinaryEncoding(Arguments formatOptions) {
    return CsvBinaryEncoding.fromId(formatOptions.getString(CSV_OPTION_BINARY_ENCODING,
      "binary (tile) data encoding - one of " + CsvBinaryEncoding.ids(), "base64"));
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
