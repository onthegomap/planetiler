package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.DataType.GET_TAG;

import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.expression.TypedGetter;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Utility that parses attribute values from source features, based on YAML config.
 */
public class TagValueProducer {
  public static final TagValueProducer EMPTY = new TagValueProducer(null);

  private final Map<String, TypedGetter> valueRetriever = new HashMap<>();

  private final Map<String, String> keyType = new HashMap<>();

  public TagValueProducer(Map<String, Object> map) {
    if (map == null) {
      return;
    }

    map.forEach((key, value) -> {
      if (value instanceof String stringType) {
        valueRetriever.put(key, DataType.from(stringType));
        keyType.put(key, stringType);
      } else if (value instanceof Map<?, ?> renameMap) {
        String inputKey = renameMap.containsKey("input") ? renameMap.get("input").toString() : key;
        var getter =
          renameMap.containsKey("type") ? DataType.from(renameMap.get("type").toString()) : DataType.GET_TAG;
        //When requesting the output value, actually retrieve the input key with the desired getter
        if (inputKey.equals(key)) {
          valueRetriever.put(key, getter);
        } else {
          valueRetriever.put(key, (withTags, requestedKey) -> getter.convertFrom(valueForKey(withTags, inputKey)));
        }
        if (renameMap.containsKey("type")) {
          keyType.put(key, renameMap.get("type").toString());
        }
      }
    });
  }

  /**
   * Returns a function that extracts the value for {@code key} from a {@link WithTags} instance.
   */
  public TypedGetter valueGetterForKey(String key) {
    return valueRetriever.getOrDefault(key, GET_TAG);
  }

  /**
   * Returns a function that extracts the value for {@code key} from a {@link WithTags} instance.
   */
  public Function<Contexts.FeaturePostMatch, Object> valueProducerForKey(String key) {
    var getter = valueGetterForKey(key);
    return context -> getter.apply(context.parent().feature(), key);
  }

  /**
   * Returns the mapped value for a key where the key is not known ahead of time.
   */
  public Object valueForKey(WithTags feature, String key) {
    return valueGetterForKey(key).apply(feature, key);
  }

  /**
   * Returns copy of {@code keyedMap} where the keys have been transformed by the parser associated with {code key}.
   */
  public <T> Map<Object, T> remapKeysByType(String key, Map<Object, T> keyedMap) {
    Map<Object, T> newMap = new LinkedHashMap<>();

    String dataType = keyType.get(key);
    UnaryOperator<Object> parser;

    if (dataType == null || (parser = DataType.from(dataType).parser()) == null) {
      newMap.putAll(keyedMap);
    } else {
      keyedMap.forEach((mapKey, value) -> newMap.put(parser.apply(mapKey), value));
    }

    return newMap;
  }

  /** Returns a new map where every tag has been transformed (or inferred) by the registered conversions. */
  public Map<String, Object> mapTags(WithTags feature) {
    if (valueRetriever.isEmpty()) {
      return feature.tags();
    } else {
      Map<String, Object> result = new HashMap<>(feature.tags());
      valueRetriever.forEach((key, retriever) -> result.put(key, retriever.apply(feature, key)));
      return result;
    }
  }
}
