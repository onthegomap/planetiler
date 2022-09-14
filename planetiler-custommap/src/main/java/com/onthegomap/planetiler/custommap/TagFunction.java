package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.expression.ConfigExpression.unescapeExpression;
import static com.onthegomap.planetiler.custommap.expression.ConfigFunction.coalesce;
import static com.onthegomap.planetiler.custommap.expression.ConfigFunction.constOf;
import static com.onthegomap.planetiler.custommap.expression.ConfigFunction.signature;

import com.onthegomap.planetiler.custommap.expression.ConfigExpression;
import com.onthegomap.planetiler.custommap.expression.ConfigFunction;
import com.onthegomap.planetiler.custommap.expression.ScriptContext;
import com.onthegomap.planetiler.custommap.expression.ScriptContextDescription;
import com.onthegomap.planetiler.expression.MultiExpression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class TagFunction<I extends ScriptContext, O> {

  private final TagValueProducer tagValueProducer;
  private final ConfigFunction.Signature<I, O> signature;

  public TagFunction(TagValueProducer tagValueProducer, ConfigFunction.Signature<I, O> signature) {
    this.tagValueProducer = tagValueProducer;
    this.signature = signature;
  }

  public static <I extends ScriptContext, O> ConfigFunction<I, O> function(Object object,
    TagValueProducer tagValueProducer, ScriptContextDescription<I> context, Class<O> outputClass) {
    return new TagFunction<>(tagValueProducer, signature(context, outputClass)).function(object).simplify();
  }

  private ConfigFunction<I, O> function(Object object) {
    if (object == null) {
      return ConfigFunction.constOf(null);
    } else if (ConfigExpression.isExpression(object)) {
      return ConfigFunction.expression(signature, ConfigExpression.extractFromEscaped(object));
    } else if (object instanceof Collection<?> collection) {
      return parseMatch(collection, true);
    } else if (object instanceof Map<?, ?> map) {
      var keys = map.keySet();
      if (keys.equals(Set.of("coalesce")) && map.get("coalesce")instanceof Collection<?> cases) {
        return coalesce(cases.stream().map(this::function).toList());
      } else if (keys.equals(Set.of("match"))) {
        return parseMatch(map.get("match"), true);
      } else if (keys.equals(Set.of("default_value", "overrides"))) {
        var match = parseMatch(map.get("overrides"), false);
        var defaultValue = function(map.get("default_value"));
        return match.withDefaultValue(defaultValue);
      }
      return parseMatch(map, true);
    } else {
      object = unescapeExpression(object);
      return constOf(TypeConversion.convert(object, signature.out()));
    }
  }

  private ConfigFunction.Match<I, O> parseMatch(Object match, boolean allowElse) {
    List<MultiExpression.Entry<ConfigFunction<I, O>>> conditions = new ArrayList<>();
    ConfigFunction<I, O> fallback = constOf(null);
    if (match instanceof Collection<?> items) {
      for (var item : items) {
        if (item instanceof Map<?, ?> map) {
          if (map.keySet().equals(Set.of("if", "value"))) {
            conditions.add(MultiExpression.entry(function(map.get("value")),
              TagCriteria.matcher(map.get("if"), tagValueProducer, signature.in())));
          } else if (allowElse && map.keySet().equals(Set.of("else"))) {
            fallback = function(map.get("else"));
            break;
          } else {
            throw new IllegalArgumentException(
              "Invalid match case. Expected if/then" + (allowElse ? " or else" : "") + ", got: " + match);
          }
        }
      }
    } else if (match instanceof Map<?, ?> map) {
      for (var entry : map.entrySet()) {
        String value = Objects.toString(entry.getValue());
        if (value.matches("^_*(default_value|otherwise|default)_*$")) {
          fallback = function(entry.getKey());
        } else {
          conditions.add(MultiExpression.entry(function(entry.getKey()),
            TagCriteria.matcher(entry.getValue(), tagValueProducer, signature.in())));
        }
      }
    } else {
      throw new IllegalArgumentException("Invalid match block. Expected a list or map, but got: " + match);
    }
    return ConfigFunction.match(signature, MultiExpression.of(List.copyOf(conditions)), fallback);
  }
}
