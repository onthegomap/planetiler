package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.DataTypes.GET_TAG;

import com.onthegomap.planetiler.custommap.expression.Contexts;
import com.onthegomap.planetiler.expression.DataTypes;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Utility that parses attribute values from source features, based on YAML config.
 */
public class TagValueProducer {

  private final Map<String, BiFunction<WithTags, String, Object>> valueRetriever = new HashMap<>();

  private final Map<String, String> keyType = new HashMap<>();
  private final List<String> remappedKeys = new ArrayList<>();

  public TagValueProducer(Map<String, Object> map) {
    if (map == null) {
      return;
    }

    map.forEach((key, value) -> {
      if (value instanceof String stringType) {
        valueRetriever.put(key, DataTypes.from(stringType));
        keyType.put(key, stringType);
        remappedKeys.add(key);
      } else if (value instanceof Map<?, ?> renameMap) {
        String output = renameMap.containsKey("output") ? renameMap.get("output").toString() : key;
        remappedKeys.add(output);
        BiFunction<WithTags, String, Object> getter =
          renameMap.containsKey("type") ? DataTypes.from(renameMap.get("type").toString()) : DataTypes.GET_TAG;
        //When requesting the output value, actually retrieve the input key with the desired getter
        valueRetriever.put(output,
          (withTags, requestedKey) -> getter.apply(withTags, key));
        if (renameMap.containsKey("type")) {
          keyType.put(output, renameMap.get("type").toString());
        }
      }
    });
  }

  /**
   * Returns a function that extracts the value for {@code key} from a {@link WithTags} instance.
   */
  public BiFunction<WithTags, String, Object> valueGetterForKey(String key) {
    return valueRetriever.getOrDefault(key, GET_TAG);
  }

  /**
   * Returns a function that extracts the value for {@code key} from a {@link WithTags} instance.
   */
  public Signature valueProducerForKey(String key) {
    var getter = valueGetterForKey(key);
    return context -> getter.apply(context.parent().feature(), key);
  }

  /**
   * Returns the mapped value for a key where the key is not known ahead-of-time.
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

    if (dataType == null || (parser = DataTypes.from(dataType).parser()) == null) {
      newMap.putAll(keyedMap);
    } else {
      keyedMap.forEach((mapKey, value) -> newMap.put(parser.apply(mapKey), value));
    }

    return newMap;
  }

  public Map<String, Object> mapTags(WithTags feature) {
    if (valueRetriever.isEmpty()) {
      return feature.tags();
    } else {
      Map<String, Object> result = new HashMap<>(feature.tags());
      valueRetriever.forEach((key, retriever) -> result.put(key, retriever.apply(feature, key)));
      return result;
    }
  }

  @FunctionalInterface
  public interface Signature extends Function<Contexts.ProcessFeature.PostMatch, Object> {}
}
