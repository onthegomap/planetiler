package com.onthegomap.planetiler.custommap.configschema;

import static com.onthegomap.planetiler.expression.Expression.matchAnyTyped;
import static com.onthegomap.planetiler.expression.Expression.matchField;

import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.expression.Expression;
import java.util.Collection;
import java.util.Map;

public class TagCriteria {

  /**
   * Returns a function that determines whether a source feature matches any of the entries in this specification
   * 
   * @param map              a map of tag criteria
   * @param tagValueProducer a TagValueProducer
   * @return a predicate which returns true if this criteria matches
   */
  public static Expression matcher(Map<String, Object> map, TagValueProducer tagValueProducer) {
    return map.entrySet()
      .stream()
      .map(entry -> tagCriterionToExpression(tagValueProducer, entry.getKey(), entry.getValue()))
      .reduce(Expression::or)
      .orElse(Expression.TRUE);
  }

  private static Expression tagCriterionToExpression(TagValueProducer tagValueProducer, String key, Object value) {

    //If only a key is provided, with no value, match any object tagged with that key.
    if (value == null) {
      return matchField(key);

      //If a collection is provided, match any of these values.
    } else if (value instanceof Collection) {
      Collection<?> values =
        (Collection<?>) value;
      return matchAnyTyped(
        key,
        tagValueProducer.getValueGetter(key),
        values.stream()
          .map(Object::toString)
          .toList());

      //Otherwise, a key and single value were passed, so match that exact tag
    } else {
      return matchAnyTyped(
        key,
        tagValueProducer.getValueGetter(key),
        value.toString());
    }
  }
}
