package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.custommap.configschema.TagValueDataType;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

public class TagValueProducer {

  private Map<String, TagValueDataType> typeMap = new HashMap<>();

  public TagValueProducer(Map<TagValueDataType, Collection<String>> map) {
    if (map == null) {
      return;
    }

    //Unpack YML representation into key->type pairs
    map.entrySet().forEach(entry -> entry.getValue().forEach(value -> typeMap.put(value, entry.getKey())));
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
