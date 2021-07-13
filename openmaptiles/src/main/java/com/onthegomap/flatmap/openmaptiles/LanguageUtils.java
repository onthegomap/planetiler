package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.ibm.icu.text.Transliterator;
import com.onthegomap.flatmap.Translations;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * This class is ported from https://github.com/openmaptiles/openmaptiles-tools/blob/master/sql/zzz_language.sql
 */
public class LanguageUtils {

  private static void putIfNotNull(Map<String, Object> dest, String key, Object value) {
    if (value != null && !value.equals("")) {
      dest.put(key, value);
    }
  }

  private static String string(Object obj) {
    return nullIfEmpty(obj == null ? null : obj.toString());
  }

  private static final Pattern NONLATIN = Pattern
    .compile("[^\\x{0000}-\\x{024f}\\x{1E00}-\\x{1EFF}\\x{0300}-\\x{036f}\\x{0259}]");

  static boolean isLatin(String string) {
    return string != null && !NONLATIN.matcher(string).find();
  }

  private static final Transliterator TO_LATIN_TRANSLITERATOR = Transliterator.getInstance("Any-Latin");

  private static String transliterate(Map<String, Object> properties) {
    String name = string(properties.get("name"));
    return name == null ? null : TO_LATIN_TRANSLITERATOR.transform(name);
  }

  private static final Pattern LETTER = Pattern.compile("[A-Za-zÀ-ÖØ-öø-ÿĀ-ɏ]+");
  private static final Pattern EMPTY_PARENS = Pattern.compile("(\\([ -.]*\\)|\\[[ -.]*])");
  private static final Pattern LEADING_TRAILING_JUNK = Pattern.compile("(^\\s*([./-]\\s*)*|(\\s+[./-])*\\s*$)");
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");

  static String removeNonLatin(String name) {
    if (name == null) {
      return null;
    }
    var matcher = LETTER.matcher(name);
    if (matcher.find()) {
      String result = matcher.replaceAll("");
      result = EMPTY_PARENS.matcher(result).replaceAll("");
      result = LEADING_TRAILING_JUNK.matcher(result).replaceAll("");
      return WHITESPACE.matcher(result).replaceAll(" ");
    }
    return name.trim();
  }

  public static Map<String, Object> getNamesWithoutTranslations(Map<String, Object> properties) {
    return getNames(properties, null);
  }

  public static Map<String, Object> getNames(Map<String, Object> properties, Translations translations) {
    Map<String, Object> result = new HashMap<>();

    String name = string(properties.get("name"));
    String intName = string(properties.get("int_name"));
    String nameEn = string(properties.get("name:en"));
    String nameDe = string(properties.get("name:de"));

    boolean isLatin = isLatin(name);
    String latin = isLatin ? name : Stream.concat(Stream.of(nameEn, intName, nameDe), getAllNames(properties))
      .filter(LanguageUtils::isLatin)
      .findFirst().orElse(null);
    if (latin == null && translations != null && translations.getShouldTransliterate()) {
      latin = transliterate(properties);
    }
    String nonLatin = isLatin ? null : removeNonLatin(name);
    if (coalesce(nonLatin, "").equals(latin)) {
      nonLatin = null;
    }

    putIfNotNull(result, "name", name);
    putIfNotNull(result, "name_en", coalesce(nameEn, name));
    putIfNotNull(result, "name_de", coalesce(nameDe, name, nameEn));
    putIfNotNull(result, "name:latin", latin);
    putIfNotNull(result, "name:nonlatin", nonLatin);
    putIfNotNull(result, "name_int", coalesce(
      intName,
      nameEn,
      latin,
      name
    ));

    if (translations != null) {
      translations.addTranslations(result, properties);
    }

    return result;
  }

  private static final Set<String> EN_DE_NAME_KEYS = Set.of("name:en", "name:de");

  private static Stream<String> getAllNames(Map<String, Object> properties) {
    return properties.entrySet().stream()
      .filter(e -> {
        String key = e.getKey();
        return key.startsWith("name:") && !EN_DE_NAME_KEYS.contains(key);
      })
      .map(Map.Entry::getValue)
      .map(LanguageUtils::string);
  }

}
