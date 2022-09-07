package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.Expression.matchAnyTyped;
import static com.onthegomap.planetiler.expression.Expression.matchField;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.custommap.expression.ConfigExpression;
import com.onthegomap.planetiler.custommap.expression.DynamicBooleanExpression;
import com.onthegomap.planetiler.custommap.expression.ScriptContext;
import com.onthegomap.planetiler.custommap.expression.ScriptContextDescription;
import com.onthegomap.planetiler.expression.Expression;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Utility that maps expressions in YAML format to {@link Expression Expressions}.
 *
 * @param <T> Input type of the expression
 */
public class TagCriteria<T extends ScriptContext> {

  private static final Pattern ESCAPED =
    Pattern.compile("^([\\s\\\\]*)\\\\(__any__|__all__)", Pattern.CASE_INSENSITIVE);

  private static final Predicate<String> IS_ANY =
    Pattern.compile("^\\s*__any__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private static final Predicate<String> IS_ALL =
    Pattern.compile("^\\s*__all__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private static final Predicate<String> IS_NOT =
    Pattern.compile("^\\s*__not__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private final TagValueProducer tagValueProducer;
  private final ScriptContextDescription<T> context;

  private TagCriteria(TagValueProducer tagValueProducer, ScriptContextDescription<T> context) {
    this.tagValueProducer = tagValueProducer;
    this.context = context;
  }

  /**
   * Returns a function that determines whether a source feature matches any of the entries in this specification
   *
   * @param <T>              Type of input the expression takes
   * @param object           a map or list of tag criteria
   * @param tagValueProducer a TagValueProducer
   * @return a predicate which returns true if this criteria matches
   */
  public static <T extends ScriptContext> Expression matcher(Object object, TagValueProducer tagValueProducer,
    ScriptContextDescription<T> context) {
    return new TagCriteria<>(tagValueProducer, context).matcher(object);
  }

  private Expression matcher(Object object) {
    return matcher(object, Expression::or);
  }

  private Expression matcher(Object object, Function<List<Expression>, Expression> collector) {
    if (object == null) {
      return Expression.FALSE;
    } else if (object instanceof String s && s.trim().equalsIgnoreCase("__any__")) {
      return Expression.TRUE;
    } else if (ConfigExpression.isExpression(object)) {
      return DynamicBooleanExpression.dynamic(ConfigExpression.extractFromEscaped(object), context);
    } else if (object instanceof Map<?, ?> map) {
      return mapMatcher(map, collector);
    } else if (object instanceof Collection<?> list) {
      return collector.apply(list.stream().map(this::matcher).toList());
    } else {
      throw new IllegalArgumentException("Unsupported object for matcher input: " + object);
    }
  }

  private Expression mapMatcher(Map<?, ?> map, Function<List<Expression>, Expression> collector) {
    return collector.apply(map.entrySet()
      .stream()
      .map(entry -> tagCriterionToExpression(entry.getKey().toString(), entry.getValue()))
      .toList());
  }

  private static boolean isListOrMap(Object object) {
    return object instanceof Map<?, ?> || object instanceof Collection<?>;
  }

  private Expression tagCriterionToExpression(String key, Object value) {
    if (IS_ANY.test(key) && isListOrMap(value)) {
      // __any__ ors together its children
      return matcher(value, Expression::or);
    } else if (IS_ALL.test(key) && isListOrMap(value)) {
      // __all__ ands together its children
      return matcher(value, Expression::and);
    } else if (IS_NOT.test(key)) {
      // __not__ negates its children
      return not(matcher(value));
    } else if (value == null || IS_ANY.test(value.toString()) ||
      (value instanceof Collection<?> values &&
        values.stream().anyMatch(d -> d != null && IS_ANY.test(d.toString().trim())))) {
      //If only a key is provided, with no value, match any object tagged with that key.
      return matchField(unescape(key));

    } else if (value instanceof Collection<?> values) {
      //If a collection is provided, match any of these values.
      return matchAnyTyped(
        unescape(key),
        tagValueProducer.valueGetterForKey(key),
        values.stream().map(TagCriteria::unescape).toList());

    } else {
      //Otherwise, a key and single value were passed, so match that exact tag
      return matchAnyTyped(
        unescape(key),
        tagValueProducer.valueGetterForKey(key),
        unescape(value));
    }
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
}
