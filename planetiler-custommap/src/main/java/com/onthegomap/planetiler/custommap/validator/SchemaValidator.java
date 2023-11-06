package com.onthegomap.planetiler.custommap.validator;

import com.fasterxml.jackson.core.JacksonException;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.custommap.ConfiguredProfile;
import com.onthegomap.planetiler.custommap.Contexts;
import com.onthegomap.planetiler.custommap.YAML;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.util.FileWatcher;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Try;
import java.io.PrintStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;

/** Verifies that a profile maps input elements map to expected output vector tile features. */
public class SchemaValidator {

  private static final String PASS_BADGE = AnsiColors.greenBackground(" PASS ");
  private static final String FAIL_BADGE = AnsiColors.redBackground(" FAIL ");

  public static void main(String[] args) {
    // let users run `verify schema.yml` as a shortcut
    String schemaFile = null;
    if (args.length > 0 && args[0].endsWith(".yml") && !args[0].startsWith("-")) {
      schemaFile = args[0];
      args = Stream.of(args).skip(1).toArray(String[]::new);
    }
    var arguments = Arguments.fromEnvOrArgs(args);
    var schema = schemaFile == null ? arguments.inputFile("schema", "Schema file") :
      arguments.inputFile("schema", "Schema file", Path.of(schemaFile));
    var watch =
      arguments.getBoolean("watch", "Watch files for changes and re-run validation when schema or spec changes", false);


    PrintStream output = System.out;
    output.println("OK");
    var paths = validateFromCli(schema, output);

    if (watch) {
      output.println();
      output.println("Watching filesystem for changes...");
      var watcher = FileWatcher.newWatcher(paths.toArray(Path[]::new));
      watcher.pollForChanges(Duration.ofMillis(300), changed -> validateFromCli(schema, output));
    }
  }

  private static boolean hasCause(Throwable t, Class<?> cause) {
    return t != null && (cause.isInstance(t) || hasCause(t.getCause(), cause));
  }

  static Set<Path> validateFromCli(Path schemaPath, PrintStream output) {
    Set<Path> pathsToWatch = new HashSet<>();
    pathsToWatch.add(schemaPath);
    output.println();
    output.println("Validating...");
    output.println();
    SchemaValidator.Result result;
    try {
      var schema = SchemaConfig.load(schemaPath);
      var examples = schema.examples();
      // examples can either be embedded in the yaml file, or referenced
      SchemaSpecification spec;
      if (examples instanceof String s) {
        var path = Path.of(s);
        if (!path.isAbsolute()) {
          path = schemaPath.resolveSibling(path);
        }
        // if referenced, make sure we watch that file for changes
        pathsToWatch.add(path);
        spec = SchemaSpecification.load(path);
      } else if (examples != null) {
        spec = YAML.convertValue(schema, SchemaSpecification.class);
      } else {
        spec = new SchemaSpecification(List.of());
      }
      result = validate(schema, spec);
    } catch (Exception exception) {
      Throwable rootCause = ExceptionUtils.getRootCause(exception);
      if (hasCause(exception, com.onthegomap.planetiler.custommap.expression.ParseException.class)) {
        output.println(AnsiColors.red("Malformed expression:\n\n" + rootCause.toString().indent(4)));
      } else if (hasCause(exception, YamlEngineException.class) || hasCause(exception, JacksonException.class)) {
        output.println(AnsiColors.red("Malformed yaml input:\n\n" + rootCause.toString().indent(4)));
      } else {
        output.println(AnsiColors.red(
          "Unexpected exception thrown:\n" + rootCause.toString().indent(4) + "\n" +
            String.join("\n", ExceptionUtils.getStackTrace(rootCause)))
          .indent(4));
      }
      return pathsToWatch;
    }
    int failed = 0, passed = 0;
    List<ExampleResult> failures = new ArrayList<>();
    for (var example : result.results) {
      if (example.ok()) {
        passed++;
        output.printf("%s %s%n", PASS_BADGE, example.example().name());
      } else {
        failed++;
        printFailure(example, output);
        failures.add(example);
      }
    }
    if (!failures.isEmpty()) {
      output.println();
      output.println("Summary of failures:");
      for (var failure : failures) {
        printFailure(failure, output);
      }
    }
    List<String> summary = new ArrayList<>();
    boolean none = (passed + failed) == 0;
    if (none || failed > 0) {
      summary.add(AnsiColors.redBold(failed + " failed"));
    }
    if (none || passed > 0) {
      summary.add(AnsiColors.greenBold(passed + " passed"));
    }
    if (none || passed > 0 && failed > 0) {
      summary.add((failed + passed) + " total");
    }
    output.println();
    output.println(String.join(", ", summary));
    return pathsToWatch;
  }

  private static void printFailure(ExampleResult example, PrintStream output) {
    output.printf("%s %s%n", FAIL_BADGE, example.example().name());
    if (example.issues.isFailure()) {
      output.println(ExceptionUtils.getStackTrace(example.issues.exception()).indent(4).stripTrailing());
    } else {
      for (var issue : example.issues().get()) {
        output.println("  ● " + issue.indent(4).strip());
      }
    }
  }

  private static Geometry parseGeometry(String geometry) {
    String wkt = switch (geometry.toLowerCase(Locale.ROOT).trim()) {
      case "point" -> "POINT (0 0)";
      case "line" -> "LINESTRING (0 0, 1 1)";
      case "polygon" -> "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))";
      default -> geometry;
    };
    try {
      return new WKTReader().read(wkt);
    } catch (ParseException e) {
      throw new IllegalArgumentException("""
        Bad geometry: "%s", must be "point" "line" "polygon" or a valid WKT string.
        """.formatted(geometry));
    }
  }

  /**
   * Returns the result of validating the profile defined by {@code schema} against the examples in
   * {@code specification}.
   */
  public static Result validate(SchemaConfig schema, SchemaSpecification specification) {
    var context = Contexts.buildRootContext(Arguments.of().silence(), schema.args());
    return validate(new ConfiguredProfile(schema, context), specification, context.config());
  }

  /** Returns the result of validating {@code profile} against the examples in {@code specification}. */
  public static Result validate(Profile profile, SchemaSpecification specification, PlanetilerConfig config) {
    var featureCollectorFactory = new FeatureCollector.Factory(config, Stats.inMemory());
    return new Result(specification.examples().stream().map(example -> new ExampleResult(example, Try.apply(() -> {
      List<String> issues = new ArrayList<>();
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
        // TODO print a diff of the input and output feature YAML representations
        for (int i = 0; i < expectedFeatures.size(); i++) {
          var expected = expectedFeatures.get(i);
          var actual = result.stream().max(proximityTo(expected)).orElseThrow();
          result.remove(actual);
          var actualTags = actual.getAttrsAtZoom(expected.atZoom());
          String prefix = "feature[%d]".formatted(i);
          validate(prefix + ".layer", issues, expected.layer(), actual.getLayer());
          validate(prefix + ".minzoom", issues, expected.minZoom(), actual.getMinZoom());
          validate(prefix + ".maxzoom", issues, expected.maxZoom(), actual.getMaxZoom());
          validate(prefix + ".minsize", issues, expected.minSize(), actual.getMinPixelSizeAtZoom(expected.atZoom()));
          validate(prefix + ".geometry", issues, expected.geometry(), GeometryType.typeOf(actual.getGeometry()));
          Set<String> tags = new TreeSet<>(actualTags.keySet());
          expected.tags().forEach((tag, value) -> {
            validate(prefix + ".tags[\"%s\"]".formatted(tag), issues, value, actualTags.get(tag), false);
            tags.remove(tag);
          });
          if (Boolean.FALSE.equals(expected.allowExtraTags())) {
            for (var tag : tags) {
              validate(prefix + ".tags[\"%s\"]".formatted(tag), issues, null, actualTags.get(tag), false);
            }
          }
        }
      }
      return issues;
    }))).toList());
  }

  private static Comparator<FeatureCollector.Feature> proximityTo(SchemaSpecification.OutputFeature expected) {
    return Comparator.comparingInt(item -> (Objects.equals(item.getLayer(), expected.layer()) ? 2 : 0) +
      (Objects.equals(GeometryType.typeOf(item.getGeometry()), expected.geometry()) ? 1 : 0));
  }

  private static <T> void validate(String field, List<String> issues, T expected, T actual, boolean ignoreWhenNull) {
    if ((!ignoreWhenNull || expected != null) && !Objects.equals(expected, actual)) {
      // handle when expected and actual are int/long or long/int
      if (expected instanceof Number && actual instanceof Number && expected.toString().equals(actual.toString())) {
        return;
      }
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

  /** Result of comparing the output vector tile feature to what was expected. */
  public record ExampleResult(
    SchemaSpecification.Example example,
    // TODO include a symmetric diff so we can pretty-print the expected/actual output diff
    Try<List<String>> issues
  ) {

    public boolean ok() {
      return issues.isSuccess() && issues.get().isEmpty();
    }
  }

  public record Result(List<ExampleResult> results) {

    public boolean ok() {
      return results.stream().allMatch(ExampleResult::ok);
    }
  }
}
