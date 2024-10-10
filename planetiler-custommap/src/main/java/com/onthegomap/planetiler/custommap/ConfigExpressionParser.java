package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.custommap.expression.ConfigExpression.*;
import static com.onthegomap.planetiler.custommap.expression.ConfigExpressionScript.unescape;

import com.onthegomap.planetiler.custommap.expression.ConfigExpression;
import com.onthegomap.planetiler.custommap.expression.ConfigExpressionScript;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import com.onthegomap.planetiler.custommap.expression.ScriptContext;
import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;
import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.util.Memoized;
import com.onthegomap.planetiler.util.Try;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Parses user-defined YAML into an {@link ConfigExpressionParser expression} that can be evaluated against an input
 * feature.
 *
 * @param <I> Input type of the expression
 */
public class ConfigExpressionParser<I extends ScriptContext> {

  private static final Memoized<EvaluateInput, ?> MEMOIZED = Memoized.memoize(arg -> ConfigExpressionParser
    .parse(arg.expression, TagValueProducer.EMPTY, arg.root.description(), arg.clazz).apply(arg.root));
  private final TagValueProducer tagValueProducer;
  private final ScriptEnvironment<I> input;

  public ConfigExpressionParser(TagValueProducer tagValueProducer, ScriptEnvironment<I> input) {
    this.tagValueProducer = tagValueProducer;
    this.input = input;
  }

  /**
   * Returns an expression parsed from user-defined YAML that can be evaluated against an input of type {@code <I>} and
   * returns output of type {@code <O>}.
   *
   * @param object           a map or list of tag criteria
   * @param tagValueProducer a TagValueProducer
   * @param <I>              Input type of the expression
   * @param <O>              Return type of the expression
   */
  public static <I extends ScriptContext, O> ConfigExpression<I, O> parse(Object object,
    TagValueProducer tagValueProducer, ScriptEnvironment<I> context, Class<O> outputClass) {
    return new ConfigExpressionParser<>(tagValueProducer, context).parse(object, outputClass);
  }

  /**
   * Attempts to evaluate {@code expression} from a yaml config, using only globally-available environmental variables
   * from the {@code root} context.
   */
  public static <T> Try<T> tryStaticEvaluate(Contexts.Root root, Object expression, Class<T> resultType) {
    if (expression == null) {
      return Try.success(null);
    }
    return MEMOIZED.tryApply(new EvaluateInput(root, expression, resultType), resultType);
  }

  private <O> ConfigExpression<I, O> parse(Object object, Class<O> output) {
    if (object == null) {
      return ConfigExpression.constOf(null);
    } else if (ConfigExpressionScript.isScript(object)) {
      return ConfigExpression.script(signature(output), ConfigExpressionScript.extractScript(object));
    } else if (object instanceof Collection<?> collection) {
      return parseMatch(collection, true, output);
    } else if (object instanceof Map<?, ?> map) {
      if (map.get("type") != null) {
        var map2 = new HashMap<>(map);
        var type = map2.remove("type");
        DataType dataType = DataType.from(Objects.toString(type));
        if (!dataType.id().equals(type)) {
          throw new ParseException("Unrecognized datatype '" + type + "' supported values: " +
            Stream.of(DataType.values()).map(DataType::id).collect(
              Collectors.joining(", ")));
        }
        var child = parse(map2, Object.class);
        return cast(signature(output), child, dataType);
      } else {
        var keys = map.keySet();
        if (keys.equals(Set.of("coalesce")) && map.get("coalesce") instanceof Collection<?> cases) {
          return coalesce(cases.stream().map(item -> parse(item, output)).toList());
        } else if (keys.equals(Set.of("match"))) {
          return parseMatch(map.get("match"), true, output);
        } else if (keys.equals(Set.of("default_value", "overrides"))) {
          var match = parseMatch(map.get("overrides"), false, output);
          var defaultValue = parse(map.get("default_value"), output);
          return match.withDefaultValue(defaultValue);
        } else if (keys.equals(Set.of("tag_value"))) {
          var tagProducer = parse(map.get("tag_value"), String.class);
          return getTag(signature(output), tagProducer);
        } else if (keys.equals(Set.of("arg_value"))) {
          var keyProducer = parse(map.get("arg_value"), String.class);
          return getArg(signature(output), keyProducer);
        } else if (keys.equals(Set.of("value"))) {
          return parse(map.get("value"), output);
        }
        try {
          return parseMatch(map, true, output);
        } catch (ParseException e) {
          throw new ParseException("Failed to parse: " + map);
        }
      }
    } else {
      object = unescape(object);
      return constOf(TypeConversion.convert(object, output));
    }
  }

  private <O> ConfigExpression.Match<I, O> parseMatch(Object match, boolean allowElse, Class<O> output) {
    List<MultiExpression.Entry<ConfigExpression<I, O>>> conditions = new ArrayList<>();
    ConfigExpression<I, O> fallback = constOf(null);
    if (match instanceof Collection<?> items) {
      for (var item : items) {
        if (item instanceof Map<?, ?> map) {
          if (map.keySet().equals(Set.of("if", "value"))) {
            conditions.add(MultiExpression.entry(parse(map.get("value"), output),
              BooleanExpressionParser.parse(map.get("if"), tagValueProducer, input)));
          } else if (allowElse && map.keySet().equals(Set.of("else"))) {
            fallback = parse(map.get("else"), output);
            break;
          } else {
            throw new ParseException(
              "Invalid match case. Expected if/then" + (allowElse ? " or else" : "") + ", got: " + match);
          }
        }
      }
    } else if (match instanceof Map<?, ?> map) {
      for (var entry : map.entrySet()) {
        String value = Objects.toString(entry.getValue());
        if (value.matches("^_*(default_value|otherwise|default)_*$")) {
          fallback = parse(entry.getKey(), output);
        } else {
          conditions.add(MultiExpression.entry(parse(entry.getKey(), output),
            BooleanExpressionParser.parse(entry.getValue(), tagValueProducer, input)));
        }
      }
    } else {
      throw new ParseException("Invalid match block. Expected a list or map, but got: " + match);
    }
    return ConfigExpression.match(signature(output), MultiExpression.of(List.copyOf(conditions)), fallback);
  }

  private <O> Signature<I, O> signature(Class<O> outputClass) {
    return new Signature<>(input, outputClass);
  }

  private record EvaluateInput(Contexts.Root root, Object expression, Class<?> clazz) {}
}
