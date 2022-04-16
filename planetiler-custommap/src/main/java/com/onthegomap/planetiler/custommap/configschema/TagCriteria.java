package com.onthegomap.planetiler.custommap.configschema;

import com.onthegomap.planetiler.custommap.TagValueProducer;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.function.Predicate;

public class TagCriteria extends HashMap<String, Object> {

  private static final long serialVersionUID = 1L;

  /**
   * Returns a function that determines whether a source feature matches any of the entries in this specification
   * 
   * @param sf source feature
   * @return a predicate which returns true if this criteria matches
   */
  public Predicate<SourceFeature> matcher(TagValueProducer tagValueProducer) {

    List<Predicate<SourceFeature>> tagTests = new ArrayList<>();

    entrySet()
      .stream()
      .forEach(
        entry -> {
          if (entry.getValue() instanceof Collection) {
            Collection<?> values =
              (Collection<?>) entry.getValue();
            tagTests
              .add((Predicate<SourceFeature>) sf -> values
                .contains(tagValueProducer.getValueProducer(entry.getKey()).apply(sf)));
          } else {
            tagTests
              .add((Predicate<SourceFeature>) sf -> entry.getValue()
                .equals(tagValueProducer.getValueProducer(entry.getKey()).apply(sf)));
          }
        });

    if (tagTests.isEmpty()) {
      return sf -> true;
    }

    Predicate<SourceFeature> test = tagTests.remove(0);
    while (!tagTests.isEmpty()) {
      test = test.or(tagTests.remove(0));
    }

    return test;
  }
}
