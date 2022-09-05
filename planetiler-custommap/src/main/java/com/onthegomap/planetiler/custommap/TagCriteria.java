package com.onthegomap.planetiler.custommap;

import static com.onthegomap.planetiler.expression.Expression.matchAnyTyped;
import static com.onthegomap.planetiler.expression.Expression.matchField;
import static com.onthegomap.planetiler.expression.Expression.not;

import com.onthegomap.planetiler.expression.Expression;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;

/**
 * Utility that maps expressions in YAML format to {@link Expression Expressions}.
 */
public class TagCriteria {
  private static final Pattern ESCAPED =
    Pattern.compile("^([\\s\\\\]*)\\\\(__any__|__all__)", Pattern.CASE_INSENSITIVE);

  private static final Predicate<String> IS_ANY =
    Pattern.compile("^\\s*__any__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private static final Predicate<String> IS_ALL =
    Pattern.compile("^\\s*__all__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();
  private static final Predicate<String> IS_NOT =
    Pattern.compile("^\\s*__not__\\s*$", Pattern.CASE_INSENSITIVE).asMatchPredicate();

  private TagCriteria() {
    //Hide implicit public constructor
  }

  /**
   * Returns a function that determines whether a source feature matches any of the entries in this specification
   *
   * @param object           a map or list of tag criteria
   * @param tagValueProducer a TagValueProducer
   * @return a predicate which returns true if this criteria matches
   */
  public static Expression matcher(Object object, TagValueProducer tagValueProducer) {
    return matcher(object, tagValueProducer, Expression::or);
  }

  private static Expression matcher(Object object, TagValueProducer tagValueProducer,
    Function<List<Expression>, Expression> collector) {
    if (object == null) {
      return Expression.FALSE;
    } else if (object instanceof String s && s.trim().equalsIgnoreCase("__any__")) {
      return Expression.TRUE;
    } else if (object instanceof Map<?, ?> map) {
      return mapMatcher(map, tagValueProducer, collector);
    } else if (object instanceof Collection<?> list) {
      return collector.apply(list.stream().map(d -> matcher(d, tagValueProducer)).toList());
    } else {
      throw new IllegalArgumentException("Unsupported object for matcher input: " + object);
    }
  }

  private static Expression mapMatcher(Map<?, ?> map, TagValueProducer tagValueProducer,
    Function<List<Expression>, Expression> collector) {
    return collector.apply(map.entrySet()
      .stream()
      .map(entry -> tagCriterionToExpression(tagValueProducer, entry.getKey().toString(), entry.getValue()))
      .toList());
  }

  private static boolean isListOrMap(Object object) {
    return object instanceof Map<?, ?> || object instanceof Collection<?>;
  }

  private static Expression tagCriterionToExpression(TagValueProducer tagValueProducer, String key, Object value) {
    if (IS_ANY.test(key) && isListOrMap(value)) {
      // __any__ ors together its children
      return matcher(value, tagValueProducer, Expression::or);
    } else if (IS_ALL.test(key) && isListOrMap(value)) {
      // __all__ ands together its children
      return matcher(value, tagValueProducer, Expression::and);
    } else if (IS_NOT.test(key)) {
      // __not__ negates its children
      return not(matcher(value, tagValueProducer));
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
