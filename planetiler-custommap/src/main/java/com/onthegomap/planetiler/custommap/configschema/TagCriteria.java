package com.onthegomap.planetiler.custommap.configschema;

import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.expression.Expression;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class TagCriteria extends HashMap<String, Object> {

  private static final long serialVersionUID = 1L;

  /**
   * Returns a function that determines whether a source feature matches any of the entries in this specification
   * 
   * @param sf source feature
   * @return a predicate which returns true if this criteria matches
   */
  public Expression matcher(TagValueProducer tagValueProducer) {

    if (isEmpty()) {
      return Expression.TRUE;
    }

    List<Expression> tagExpressions = new ArrayList<>();

    entrySet()
      .stream()
      .forEach(
        entry -> {
          if (entry.getValue() instanceof Collection) {
            Collection<?> values =
              (Collection<?>) entry.getValue();;
            tagExpressions.add(
              Expression.matchAnyTyped(
                entry.getKey(),
                tagValueProducer.getValueGetter(entry.getKey()),
                values.stream().map(Object::toString).toList()));
          } else {
            tagExpressions.add(
              Expression.matchAnyTyped(
                entry.getKey(),
                tagValueProducer.getValueGetter(entry.getKey()),
                entry.getValue().toString()));
          }
        });

    return Expression.or(tagExpressions);
  }
}
