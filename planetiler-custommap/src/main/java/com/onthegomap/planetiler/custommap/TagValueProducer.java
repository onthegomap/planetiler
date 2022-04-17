package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.custommap.configschema.TagValueDataType;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

public class TagValueProducer {

  private final Map<String, TagValueDataType> typeMap;

  public TagValueProducer(Map<String, TagValueDataType> map) {
    if (map == null) {
      typeMap = Collections.emptyMap();
      return;
    }

    typeMap = map;
  }

  public Function<SourceFeature, Object> getValueProducer(String key) {
    if (typeMap.containsKey(key)) {
      TagValueDataType dataType = typeMap.get(key);
      switch (dataType) {
        case BOOLEAN:
          return sf -> sf.getBoolean(key);
        case DIRECTION:
          return sf -> sf.getDirection(key);
        case LONG:
          return sf -> sf.getLong(key);
        case STRING:
          return sf -> sf.getString(key);
      }
    }
    return sf -> sf.getTag(key);
  }
}
