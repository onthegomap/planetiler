/*
Copyright (c) 2016, KlokanTech.com & OpenMapTiles contributors.
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
package com.onthegomap.flatmap.openmaptiles;

import static com.onthegomap.flatmap.openmaptiles.Utils.coalesce;
import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

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

  private static String transliterate(Map<String, Object> tags) {
    return Translations.transliterate(string(tags.get("name")));
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

  public static Map<String, Object> getNamesWithoutTranslations(Map<String, Object> tags) {
    return getNames(tags, null);
  }

  public static Map<String, Object> getNames(Map<String, Object> tags, Translations translations) {
    Map<String, Object> result = new HashMap<>();

    String name = string(tags.get("name"));
    String intName = string(tags.get("int_name"));
    String nameEn = string(tags.get("name:en"));
    String nameDe = string(tags.get("name:de"));

    boolean isLatin = isLatin(name);
    String latin = isLatin ? name : Stream.concat(Stream.of(nameEn, intName, nameDe), getAllNames(tags))
      .filter(LanguageUtils::isLatin)
      .findFirst().orElse(null);
    if (latin == null && translations != null && translations.getShouldTransliterate()) {
      latin = transliterate(tags);
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
      translations.addTranslations(result, tags);
    }

    return result;
  }

  private static final Set<String> EN_DE_NAME_KEYS = Set.of("name:en", "name:de");

  private static Stream<String> getAllNames(Map<String, Object> tags) {
    return tags.entrySet().stream()
      .filter(e -> {
        String key = e.getKey();
        return key.startsWith("name:") && !EN_DE_NAME_KEYS.contains(key);
      })
      .map(Map.Entry::getValue)
      .map(LanguageUtils::string);
  }

}
