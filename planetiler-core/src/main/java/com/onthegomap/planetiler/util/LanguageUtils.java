package com.onthegomap.planetiler.util;

import java.util.IllformedLocaleException;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;

public class LanguageUtils {
  /**
   * Name tags that should be eligible for finding a latin name.
   * 
   * @see https://wiki.openstreetmap.org/wiki/Multilingual_names
   * @deprecated Use {@code isValidLanguageTag(tag))}
   */
  @Deprecated(forRemoval = true)
  public static final Predicate<String> VALID_NAME_TAGS =
    Pattern
      .compile("^name:[a-z]{2,3}(-[A-Z][a-z]{3})?([-_](x-)?[a-z]{2,})?(-([A-Z]{2}|\\d{3}))?$")
      .asMatchPredicate();
  // See https://github.com/onthegomap/planetiler/issues/86
  // Match strings that only contain latin characters.
  private static final Predicate<String> ONLY_LATIN = Pattern
    .compile("^[\\P{IsLetter}[\\p{IsLetter}&&\\p{IsLatin}]]+$")
    .asMatchPredicate();
  // Match only latin letters
  private static final Pattern LATIN_LETTER = Pattern.compile("[\\p{IsLetter}&&\\p{IsLatin}]+");
  private static final Pattern EMPTY_PARENS = Pattern.compile("(\\([ -.]*\\)|\\[[ -.]*])");
  private static final Pattern LEADING_TRAILING_JUNK = Pattern.compile("((^[\\s./-]*)|([\\s./-]*$))");
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");
  public static final Set<String> EN_DE_NAME_KEYS = Set.of("name:en", "name:de");

  private LanguageUtils() {
    throw new IllegalStateException("Utility class");
  }

  public static void putIfNotEmpty(Map<String, Object> dest, String key, Object value) {
    if (value != null && !value.equals("")) {
      dest.put(key, value);
    }
  }

  public static String nullIfEmpty(String a) {
    return (a == null || a.isEmpty()) ? null : a;
  }

  /**
   * @deprecated Use {@code OmtLanguageUtils.string()}
   */
  @Deprecated(forRemoval = true)
  public static String string(Object obj) {
    return nullIfEmpty(obj == null ? null : obj.toString());
  }

  public static boolean containsOnlyLatinCharacters(String string) {
    return string != null && ONLY_LATIN.test(string);
  }

  /**
   * @deprecated Use {@code Translations.transliterate(string(tags.get("name")))}
   */
  @Deprecated(forRemoval = true)
  public static String transliteratedName(Map<String, Object> tags) {
    return Translations.transliterate(string(tags.get("name")));
  }

  public static String removeLatinCharacters(String name) {
    if (name == null) {
      return null;
    }
    var matcher = LATIN_LETTER.matcher(name);
    if (matcher.find()) {
      String result = matcher.replaceAll("");
      // if the name was "<nonlatin text> (<latin description)"
      // or "<nonlatin text> - <latin description>"
      // then remove any of those extra characters now
      result = EMPTY_PARENS.matcher(result).replaceAll("");
      result = LEADING_TRAILING_JUNK.matcher(result).replaceAll("");
      result = WHITESPACE.matcher(result).replaceAll(" ").trim();
      return result.isBlank() ? null : result;
    }
    return name.trim();
  }

  /**
   * Returns whether a BCP 47 language tag conforms OpenStreetMap's conventions for a localized name subkey.
   */
  public static boolean isValidLanguageTag(String tag) {
    // Locale maps the empty string to "und", but we wouldn't use a subkey for that. 
    if (tag == null || tag.isEmpty()) {
      return false;
    }

    // A subkey starting with a capital letter likely qualifies a name by the organization that assigned it.
    if (!Character.isLowerCase(tag.charAt(0))) {
      return false;
    }

    // Check whether the code is well-formed according to BCP 47.
    Locale locale;
    try {
      locale = new Locale.Builder().setLanguageTag(tag).build();
    } catch (IllformedLocaleException e) {
      return false;
    }

    String lang = locale.getLanguage();
    // BCP 47 technically allows a language code up to 8 characters long for future use.
    // Such a long subkey is likely to be something other than a language tag.
    return !lang.isEmpty() && lang.length() <= 3;
  }

  /**
   * Returns whether a key conforms to OpenStreetMap's conventions for a localized name subkey, but not the primary name
   * key or a subkey of some other kind of name such as the short name.
   */
  public static boolean isValidOsmNameTag(String tag) {
    return tag.startsWith("name:") && isValidLanguageTag(tag.substring(5));
  }

}
