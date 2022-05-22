package com.onthegomap.planetiler.custommap.configschema;

import static com.onthegomap.planetiler.expression.Expression.matchAnyTyped;
import static com.onthegomap.planetiler.expression.Expression.matchField;

import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.expression.Expression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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

    if (map.isEmpty()) {
      return Expression.TRUE;
    }

    List<Expression> tagExpressions = new ArrayList<>();

    map.entrySet()
      .stream()
      .forEach(
        entry -> {
          if (entry.getValue() == null) {
            //If only a key is provided, with no value, match any object tagged with that key.
            tagExpressions.add(
              matchField(entry.getKey()));
          } else if (entry.getValue() instanceof Collection) {
            Collection<?> values =
              (Collection<?>) entry.getValue();
            tagExpressions.add(
              matchAnyTyped(
                entry.getKey(),
                tagValueProducer.getValueGetter(entry.getKey()),
                values.stream()
                  .map(Object::toString)
                  .toList()));
          } else {
            tagExpressions.add(
              matchAnyTyped(
                entry.getKey(),
                tagValueProducer.getValueGetter(entry.getKey()),
                entry.getValue().toString()));
          }
        });

    return Expression.or(tagExpressions);
  }
}
