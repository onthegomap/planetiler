package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.custommap.expression.Contexts;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Parse;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * Utility that parses attribute values from source features, based on YAML config.
 */
public class TagValueProducer {

  private static final String STRING_DATATYPE = "string";
  private static final String BOOLEAN_DATATYPE = "boolean";
  private static final String DIRECTION_DATATYPE = "direction";
  private static final String LONG_DATATYPE = "long";

  private static final BiFunction<WithTags, String, Object> DEFAULT_GETTER = WithTags::getTag;

  private final Map<String, BiFunction<WithTags, String, Object>> valueRetriever = new HashMap<>();

  private final Map<String, String> keyType = new HashMap<>();

  private static final Map<String, BiFunction<WithTags, String, Object>> inputGetter =
    Map.of(
      STRING_DATATYPE, WithTags::getString,
      BOOLEAN_DATATYPE, WithTags::getBoolean,
      DIRECTION_DATATYPE, WithTags::getDirection,
      LONG_DATATYPE, WithTags::getLong
    );

  private static final Map<String, UnaryOperator<Object>> inputParse =
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
        valueRetriever.put(key, inputGetter.get(stringType));
        keyType.put(key, stringType);
      } else if (value instanceof Map<?, ?> renameMap) {
        String output = renameMap.containsKey("output") ? renameMap.get("output").toString() : key;
        BiFunction<WithTags, String, Object> getter =
          renameMap.containsKey("type") ? inputGetter.get(renameMap.get("type").toString()) : DEFAULT_GETTER;
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
    return valueRetriever.getOrDefault(key, DEFAULT_GETTER);
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

    if (dataType == null || (parser = inputParse.get(dataType)) == null) {
      newMap.putAll(keyedMap);
    } else {
      keyedMap.forEach((mapKey, value) -> newMap.put(parser.apply(mapKey), value));
    }

    return newMap;
  }

  @FunctionalInterface
  public interface Signature extends Function<Contexts.ProcessFeature.PostMatch, Object> {}
}
