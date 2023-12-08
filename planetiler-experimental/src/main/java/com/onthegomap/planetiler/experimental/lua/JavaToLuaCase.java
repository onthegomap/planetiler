package com.onthegomap.planetiler.experimental.lua;

import static java.lang.Character.isDigit;
import static java.lang.Character.isLowerCase;
import static java.lang.Character.isUpperCase;

import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Converts conventional javaMemberNames to lua_member_names, and lua keywords to uppercase.
 */
public class JavaToLuaCase {
  public static final Set<String> LUA_KEYWORDS = Set.of(
    "and", "break", "do", "else", "elseif",
    "end", "false", "for", "function", "if",
    "in", "local", "nil", "not", "or",
    "repeat", "return", "then", "true", "until", "while"
  );
  private static final String LOWER = "[a-z]";
  private static final String DIGIT = "\\d";
  private static final String UPPER = "[A-Z]";
  private static final String UPPER_OR_DIGIT = "[" + UPPER + DIGIT + "]";
  private static final List<Boundary> BOUNDARIES = List.of(
    // getUTF8string -> get_utf8_string
    new Boundary(DIGIT, LOWER),
    // fooBar -> foo_bar
    new Boundary(LOWER, UPPER),
    // ASCIIString -> ascii_string, UTF8String -> utf8_string
    new Boundary(UPPER_OR_DIGIT, UPPER + LOWER)
  );
  private static final List<Pattern> BOUNDARY_PATTERNS = BOUNDARIES.stream()
    .map(b -> Pattern.compile("(" + b.prev + ")(" + b.next + ")"))
    .toList();

  public static boolean isLowerCamelCase(String fieldName) {
    var chars = fieldName.toCharArray();
    if (!isLowerCase(chars[0])) {
      return false;
    }
    boolean upper = false, lower = false, underscore = false;
    for (char c : chars) {
      upper |= isUpperCase(c) || isDigit(c);
      lower |= isLowerCase(c);
      underscore |= c == '_';
    }
    return upper && lower && !underscore;
  }

  public static String transformMemberName(String fieldName) {
    if (isLowerCamelCase(fieldName)) {
      fieldName = camelToSnake(fieldName);
    }
    if (LUA_KEYWORDS.contains(fieldName)) {
      fieldName = Objects.requireNonNull(fieldName).toUpperCase(Locale.ROOT);
    }
    return fieldName;
  }

  private static String camelToSnake(String fieldName) {
    for (Pattern pattern : BOUNDARY_PATTERNS) {
      fieldName = pattern.matcher(fieldName).replaceAll("$1_$2");
    }
    return fieldName.toLowerCase();
  }

  private record Boundary(String prev, String next) {}
}
