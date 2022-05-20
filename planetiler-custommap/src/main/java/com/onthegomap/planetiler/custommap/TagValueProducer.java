package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Parse;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public class TagValueProducer {

  static final String STRING_DATATYPE = "string";
  static final String BOOLEAN_DATATYPE = "boolean";
  static final String DIRECTION_DATATYPE = "direction";
  static final String LONG_DATATYPE = "long";

  static final BiFunction<WithTags, String, Object> DEFAULT_GETTER = WithTags::getTag;

  final Map<String, BiFunction<WithTags, String, Object>> valueRetriever = new HashMap<>();

  final Map<String, String> keyType = new HashMap<>();

  static final Map<String, BiFunction<WithTags, String, Object>> dataTypeGetter =
    Map.of(
      STRING_DATATYPE, WithTags::getString,
      BOOLEAN_DATATYPE, WithTags::getBoolean,
      DIRECTION_DATATYPE, WithTags::getDirection,
      LONG_DATATYPE, WithTags::getLong
    );

  static final Map<String, UnaryOperator<Object>> dataTypeParse =
    Map.of(
      STRING_DATATYPE, s -> s,
      BOOLEAN_DATATYPE, Parse::bool,
      DIRECTION_DATATYPE, Parse::direction,
      LONG_DATATYPE, Parse::parseLong
    );

  public TagValueProducer(Map<String, Object> map) {
    if (map == null) {
      return;
    }

    map.forEach((key, value) -> {
      if (value instanceof String stringType) {
        valueRetriever.put(key, dataTypeGetter.get(stringType));
        keyType.put(key, stringType);
      } else if (value instanceof Map<?, ?> renameMap) {
        String output = renameMap.containsKey("output") ? renameMap.get("output").toString() : key;
        BiFunction<WithTags, String, Object> getter =
          renameMap.containsKey("type") ? dataTypeGetter.get(renameMap.get("type").toString()) : DEFAULT_GETTER;
        //When requesting the output value, actually retrieve the input key with the desired getter
        valueRetriever.put(output,
          (withTags, requestedKey) -> getter.apply(withTags, key));
        if (renameMap.containsKey("type")) {
          keyType.put(output, renameMap.get("type").toString());
        }
      }
    });
  }

  public BiFunction<WithTags, String, Object> getValueGetter(String key) {
    return valueRetriever.getOrDefault(key, DEFAULT_GETTER);
  }

  public Function<WithTags, Object> getValueProducer(String key) {
    return withTags -> getValueGetter(key).apply(withTags, key);
  }

  public <T extends Object> Map<Object, T> remapKeysByType(String key, Map<Object, T> keyedMap) {
    Map<Object, T> newMap = new LinkedHashMap<>();

    String dataType = keyType.get(key);

    keyedMap.forEach((mapKey, value) -> {
      if (dataType == null) {
        newMap.put(mapKey, value);
      } else {
        var parser = dataTypeParse.get(dataType);
        if (parser == null) {
          newMap.put(mapKey, value);
        } else {
          newMap.put(parser.apply(mapKey), value);
        }
      }
    });

    return newMap;
  }
}
