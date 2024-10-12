package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.Expression.matchAnyTyped;
import static com.onthegomap.planetiler.expression.Expression.matchField;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.custommap.expression.BooleanExpressionScript;
import com.onthegomap.planetiler.custommap.expression.ConfigExpressionScript;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import com.onthegomap.planetiler.custommap.expression.ScriptContext;
import com.onthegomap.planetiler.custommap.expression.ScriptEnvironment;
import com.onthegomap.planetiler.expression.Expression;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Parses user-defined YAML into boolean {@link Expression expressions} that can be evaluated against an input feature.
 *
 * @param <T> Input type of the expression
 */
public class BooleanExpressionParser<T extends ScriptContext> {

  private static final Pattern ESCAPED =
    Pattern.compile("^([\\s\\\\]*)\\\\(__any__|__all__)", Pattern.CASE_INSENSITIVE);

  private static final Predicate<String> IS_ANY =
    Pattern.compile("^\\s*__any__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private static final Predicate<String> IS_ALL =
    Pattern.compile("^\\s*__all__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private static final Predicate<String> IS_NOT =
    Pattern.compile("^\\s*__not__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private final TagValueProducer tagValueProducer;
  private final ScriptEnvironment<T> context;

  private BooleanExpressionParser(TagValueProducer tagValueProducer, ScriptEnvironment<T> context) {
    this.tagValueProducer = tagValueProducer;
    this.context = context;
  }

  /**
   * Returns a boolean expression that determines whether a source feature matches a criteria defined in yaml config.
   *
   * @param <T>              Type of input the expression takes
   * @param object           a map or list of tag criteria
   * @param tagValueProducer a TagValueProducer
   * @return a predicate which returns true if this criteria matches
   */
  public static <T extends ScriptContext> Expression parse(Object object, TagValueProducer tagValueProducer,
    ScriptEnvironment<T> context) {
    return new BooleanExpressionParser<>(tagValueProducer, context).parse(object);
  }

  private static boolean isListOrMap(Object object) {
    return object instanceof Map<?, ?> || object instanceof Collection<?>;
  }

  private static String unescape(String s) {
    var matcher = ESCAPED.matcher(s);
    if (matcher.matches()) {
      return matcher.replaceFirst("$1$2");
    }
    return s;
  }

  private static Object unescape(Object o) {
    if (o instanceof String s) {
      return unescape(s);
    }
    return o;
  }

  private Expression parse(Object object) {
    return parse(object, Expression::or);
  }

  private Expression parse(Object object, Function<List<Expression>, Expression> collector) {
    if (object == null) {
      return Expression.FALSE;
    } else if (object instanceof String s && s.trim().equalsIgnoreCase("__any__")) {
      return Expression.TRUE;
    } else if (ConfigExpressionScript.isScript(object)) {
      return BooleanExpressionScript.script(ConfigExpressionScript.extractScript(object), context);
    } else if (object instanceof Map<?, ?> map) {
      return parseMapMatch(map, collector);
    } else if (object instanceof Collection<?> list) {
      return collector.apply(list.stream().map(this::parse).toList());
    } else {
      throw new ParseException("Unsupported object for matcher input: " + object);
    }
  }

  private Expression parseMapMatch(Map<?, ?> map, Function<List<Expression>, Expression> collector) {
    return collector.apply(map.entrySet()
      .stream()
      .map(entry -> tagCriterionToExpression(entry.getKey().toString(), entry.getValue()))
      .toList());
  }

  private Expression tagCriterionToExpression(String key, Object value) {
    if (IS_ANY.test(key) && isListOrMap(value)) {
      // __any__ ors together its children
      return parse(value, Expression::or);
    } else if (IS_ALL.test(key) && isListOrMap(value)) {
      // __all__ ands together its children
      return parse(value, Expression::and);
    } else if (IS_NOT.test(key)) {
      // __not__ negates its children
      return not(parse(value));
    } else {
      //If only a key is provided, with no value, match any object tagged with that key.
      boolean isAny = value == null || IS_ANY.test(value.toString()) ||
        (value instanceof Collection<?> values &&
          values.stream().anyMatch(d -> d != null && IS_ANY.test(d.toString().trim())));
      //If a collection or single item are provided, match any of these values.
      List<?> values = (value instanceof Collection<?> items ? items : value == null ? List.of() : List.of(value))
        .stream().map(BooleanExpressionParser::unescape).toList();
      if (ConfigExpressionScript.isScript(key)) {
        var expression = ConfigExpressionScript.parse(ConfigExpressionScript.extractScript(key), context);
        if (isAny) {
          values = List.of();
        }
        return matchAnyTyped(null, expression, values);
      }
      String field = unescape(key);
      if (isAny) {
        return matchField(field);
      } else {
        return matchAnyTyped(field, tagValueProducer.valueGetterForKey(key), values);
      }
    }
  }
}
