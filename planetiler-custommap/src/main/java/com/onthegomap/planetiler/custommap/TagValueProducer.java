package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.reader.WithTags;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TagValueProducer {

  static final String STRING_DATATYPE = "string";
  static final String BOOLEAN_DATATYPE = "boolean";
  static final String DIRECTION_DATATYPE = "direction";
  static final String LONG_DATATYPE = "long";

  static final Map<String, BiFunction<WithTags, String, Object>> valueRetriever = new HashMap<>();

  static final Map<String, BiFunction<WithTags, String, Object>> dataTypeGetter =
    Map.of(
      STRING_DATATYPE, WithTags::getString,
      BOOLEAN_DATATYPE, WithTags::getBoolean,
      DIRECTION_DATATYPE, WithTags::getDirection,
      LONG_DATATYPE, WithTags::getLong
    );

  public TagValueProducer(Map<String, Object> map) {
    if (map == null) {
      return;
    }

    map.forEach((key, value) -> {
      if (value instanceof String stringType) {
        valueRetriever.put(key, dataTypeGetter.get(value));
      } else if (value instanceof Map<?, ?> renameMap) {
        String output = renameMap.containsKey("output") ? renameMap.get("output").toString() : key;
        BiFunction<WithTags, String, Object> getter =
          renameMap.containsKey("type") ? dataTypeGetter.get(renameMap.get("type").toString()) : WithTags::getTag;
        //When requesting the output value, actually retrieve the input key with the desired getter
        valueRetriever.put(output,
          (withTags, requestedKey) -> getter.apply(withTags, key));
      }
    });
  }

  public BiFunction<WithTags, String, Object> getValueGetter(String key) {
    return valueRetriever.getOrDefault(key, WithTags::getTag);
  }

  public Function<WithTags, Object> getValueProducer(String key) {
    return withTags -> getValueGetter(key).apply(withTags, key);
  }
}
