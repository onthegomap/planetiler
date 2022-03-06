/*
Copyright (c) 2021, MapTiler.com & OpenMapTiles contributors.
All rights reserved.

Code license: BSD 3-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Design license: CC-BY 4.0

See https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md for details on usage
*/
package com.onthegomap.planetiler.basemap.util;

import static com.onthegomap.planetiler.basemap.util.Utils.coalesce;
import static com.onthegomap.planetiler.basemap.util.Utils.nullIfEmpty;

import com.onthegomap.planetiler.util.Translations;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * Utilities to extract common name fields (name, name_en, name_de, name:latin, name:nonlatin,
 * name_int) that the OpenMapTiles schema uses across any map element with a name.
 *
 * <p>Ported from <a
 * href="https://github.com/openmaptiles/openmaptiles-tools/blob/master/sql/zzz_language.sql">openmaptiles-tools</a>.
 */
public class LanguageUtils {

  private static final Pattern NONLATIN =
      Pattern.compile("[^\\x{0000}-\\x{024f}\\x{1E00}-\\x{1EFF}\\x{0300}-\\x{036f}\\x{0259}]");
  private static final Pattern LETTER = Pattern.compile("[A-Za-zÀ-ÖØ-öø-ÿĀ-ɏ]+");
  private static final Pattern EMPTY_PARENS = Pattern.compile("(\\([ -.]*\\)|\\[[ -.]*])");
  private static final Pattern LEADING_TRAILING_JUNK =
      Pattern.compile("(^\\s*([./-]\\s*)*|(\\s+[./-])*\\s*$)");
  private static final Pattern WHITESPACE = Pattern.compile("\\s+");
  private static final Set<String> EN_DE_NAME_KEYS = Set.of("name:en", "name:de");

  private static void putIfNotEmpty(Map<String, Object> dest, String key, Object value) {
    if (value != null && !value.equals("")) {
      dest.put(key, value);
    }
  }

  private static String string(Object obj) {
    return nullIfEmpty(obj == null ? null : obj.toString());
  }

  static boolean containsOnlyLatinCharacters(String string) {
    return string != null && !NONLATIN.matcher(string).find();
  }

  private static String transliteratedName(Map<String, Object> tags) {
    return Translations.transliterate(string(tags.get("name")));
  }

  static String removeLatinCharacters(String name) {
    if (name == null) {
      return null;
    }
    var matcher = LETTER.matcher(name);
    if (matcher.find()) {
      String result = matcher.replaceAll("");
      // if the name was "<nonlatin text> (<latin description)"
      // or "<nonlatin text> - <latin description>"
      // then remove any of those extra characters now
      result = EMPTY_PARENS.matcher(result).replaceAll("");
      result = LEADING_TRAILING_JUNK.matcher(result).replaceAll("");
      return WHITESPACE.matcher(result).replaceAll(" ");
    }
    return name.trim();
  }

  /**
   * Returns a map with default name attributes (name, name_en, name_de, name:latin, name:nonlatin,
   * name_int) that every element should have, derived from name, int_name, name:en, and name:de
   * tags on the input element.
   *
   * <ul>
   *   <li>name is the original name value from the element
   *   <li>name_en is the original name:en value from the element, or name if missing
   *   <li>name_de is the original name:de value from the element, or name/ name_en if missing
   *   <li>name:latin is the first of name, int_name, or any name: attribute that contains only
   *       latin characters
   *   <li>name:nonlatin is any nonlatin part of name if present
   *   <li>name_int is the first of int_name name:en name:latin name
   * </ul>
   */
  public static Map<String, Object> getNamesWithoutTranslations(Map<String, Object> tags) {
    return getNames(tags, null);
  }

  /**
   * Returns a map with default name attributes that {@link #getNamesWithoutTranslations(Map)} adds,
   * but also translations for every language that {@code translations} is configured to handle.
   */
  public static Map<String, Object> getNames(Map<String, Object> tags, Translations translations) {
    Map<String, Object> result = new HashMap<>();

    String name = string(tags.get("name"));
    String intName = string(tags.get("int_name"));
    String nameEn = string(tags.get("name:en"));
    String nameDe = string(tags.get("name:de"));

    boolean isLatin = containsOnlyLatinCharacters(name);
    String latin =
        isLatin
            ? name
            : Stream.concat(
                    Stream.of(nameEn, intName, nameDe),
                    getAllNameTranslationsBesidesEnglishAndGerman(tags))
                .filter(LanguageUtils::containsOnlyLatinCharacters)
                .findFirst()
                .orElse(null);
    if (latin == null && translations != null && translations.getShouldTransliterate()) {
      latin = transliteratedName(tags);
    }
    String nonLatin = isLatin ? null : removeLatinCharacters(name);
    if (coalesce(nonLatin, "").equals(latin)) {
      nonLatin = null;
    }

    putIfNotEmpty(result, "name", name);
    putIfNotEmpty(result, "name_en", coalesce(nameEn, name));
    putIfNotEmpty(result, "name_de", coalesce(nameDe, name, nameEn));
    putIfNotEmpty(result, "name:latin", latin);
    putIfNotEmpty(result, "name:nonlatin", nonLatin);
    putIfNotEmpty(result, "name_int", coalesce(intName, nameEn, latin, name));

    if (translations != null) {
      translations.addTranslations(result, tags);
    }

    return result;
  }

  private static Stream<String> getAllNameTranslationsBesidesEnglishAndGerman(
      Map<String, Object> tags) {
    return tags.entrySet().stream()
        .filter(
            e -> {
              String key = e.getKey();
              return key.startsWith("name:") && !EN_DE_NAME_KEYS.contains(key);
            })
        .map(Map.Entry::getValue)
        .map(LanguageUtils::string);
  }
}
