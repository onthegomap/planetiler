package com.onthegomap.planetiler.custommap.validator;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.ConfiguredProfile;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Format;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import org.geotools.geometry.jts.WKTReader2;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;

public class SchemaValidator {

  private static Geometry parseGeometry(String geometry) {
    String wkt = switch (geometry.toLowerCase(Locale.ROOT).trim()) {
      case "point" -> "POINT (0 0)";
      case "line" -> "LINESTRING (0 0, 1 1)";
      case "polygon" -> "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
      default -> geometry;
    };
    try {
      return new WKTReader2().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("""
        Bad geometry: "%s", must be "point" "line" "polygon" or a valid WKT string.
        """.formatted(geometry));
    }
  }

  public static Result validate(SchemaConfig schema, SchemaSpecification specification) {
    var profile = new ConfiguredProfile(schema);
    var featureCollectorFactory = new FeatureCollector.Factory(PlanetilerConfig.defaults(), Stats.inMemory());
    return new Result(specification.examples().stream().map(example -> {
      List<String> issues = new ArrayList<>();
      Exception exception = null;
      try {
        var input = example.input();
        var expectedFeatures = example.output();
        var geometry = parseGeometry(input.geometry());
        var feature = SimpleFeature.create(geometry, input.tags(), input.source(), null, 0);
        var collector = featureCollectorFactory.get(feature);
        profile.processFeature(feature, collector);
        List<FeatureCollector.Feature> result = new ArrayList<>();
        collector.forEach(result::add);
        if (result.size() != expectedFeatures.size()) {
          issues.add(
            "Different number of elements, expected=%s actual=%s".formatted(expectedFeatures.size(), result.size()));
        } else {
          for (int i = 0; i < expectedFeatures.size(); i++) {
            var expected = expectedFeatures.get(i);
            var actual = result.stream().max(proximityTo(expected)).orElseThrow();
            result.remove(actual);
            var actualTags = actual.getAttrsAtZoom(expected.atZoom());
            String prefix = "feature[%d]".formatted(i);
            validate(prefix + ".layer", issues, expected.layer(), actual.getLayer());
            validate(prefix + ".minzoom", issues, expected.minZoom(), actual.getMinZoom());
            validate(prefix + ".maxzoom", issues, expected.maxZoom(), actual.getMaxZoom());
            validate(prefix + ".geometry", issues, expected.geometry(), GeometryType.valueOf(actual.getGeometry()));
            expected.tags().forEach((tag, value) -> {
              validate(prefix + ".tags[\"%s\"]".formatted(tag), issues, value, actualTags.get(tag), false);
            });
          }
        }
      } catch (Exception e) {
        exception = e;
      }
      return new ExampleResult(example, Optional.ofNullable(exception), issues);
    }).toList());
  }

  private static Comparator<FeatureCollector.Feature> proximityTo(SchemaSpecification.OutputFeature expected) {
    return Comparator.comparingInt(item -> (Objects.equals(item.getLayer(), expected.layer()) ? 2 : 0) +
      (Objects.equals(GeometryType.valueOf(item.getGeometry()), expected.geometry()) ? 1 : 0));
  }

  private static <T> void validate(String field, List<String> issues, T expected, T actual, boolean ignoreWhenNull) {
    if ((!ignoreWhenNull || expected != null) && !Objects.equals(expected, actual)) {
      issues.add("%s: expected <%s> actual <%s>".formatted(field, format(expected), format(actual)));
    }
  }

  private static String format(Object o) {
    if (o == null) {
      return "null";
    } else if (o instanceof String s) {
      return Format.quote(s);
    } else {
      return o.toString();
    }
  }

  private static <T> void validate(String field, List<String> issues, T expected, T actual) {
    validate(field, issues, expected, actual, true);
  }

  public record ExampleResult(
    SchemaSpecification.Example example,
    Optional<Exception> exception,
    // TODO include a symmetric diff so we can pretty-print the expected/actual output diff
    List<String> issues
  ) {

    public boolean ok() {
      return exception.isEmpty() && issues.isEmpty();
    }
  }

  public record Result(List<ExampleResult> results) {

    public boolean ok() {
      return results.stream().allMatch(ExampleResult::ok);
    }
  }
}
