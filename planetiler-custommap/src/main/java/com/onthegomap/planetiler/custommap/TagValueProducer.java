package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.reader.WithTags;
import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

public class TagValueProducer {

  static final String STRING_DATATYPE = "string";
  static final String BOOLEAN_DATATYPE = "boolean";
  static final String DIRECTION_DATATYPE = "direction";
  static final String LONG_DATATYPE = "long";

  static final Map<String, BiFunction<WithTags, String, Object>> dataTypeGetter =
    Collections.unmodifiableMap(Map.of(
      STRING_DATATYPE, WithTags::getString,
      BOOLEAN_DATATYPE, WithTags::getBoolean,
      DIRECTION_DATATYPE, WithTags::getDirection,
      LONG_DATATYPE, WithTags::getLong
    ));

  private final Map<String, String> typeMap;

  public TagValueProducer(Map<String, String> map) {
    if (map == null) {
      typeMap = Collections.emptyMap();
      return;
    }

    typeMap = map;
  }

  public BiFunction<WithTags, String, Object> getValueGetter(String key) {
    var dataType = typeMap.get(key);
    return dataTypeGetter.get(dataType == null ? STRING_DATATYPE : dataType);
  }

  public Function<WithTags, Object> getValueProducer(String key) {
    return switch (typeMap.get(key)) {
      case BOOLEAN_DATATYPE -> sf -> sf.getBoolean(key);
      case DIRECTION_DATATYPE -> sf -> sf.getDirection(key);
      case LONG_DATATYPE -> sf -> sf.getLong(key);
      case STRING_DATATYPE -> sf -> sf.getString(key);
      default -> sf -> sf.getTag(key);
    };
  }
}
