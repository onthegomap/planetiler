package com.onthegomap.planetiler.custommap.validator;

import com.fasterxml.jackson.core.JacksonException;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.ConfiguredProfile;
import com.onthegomap.planetiler.custommap.Contexts;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.util.YAML;
import com.onthegomap.planetiler.validator.BaseSchemaValidator;
import com.onthegomap.planetiler.validator.SchemaSpecification;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;

public class SchemaValidator extends BaseSchemaValidator {

  private final Path schemaPath;

  SchemaValidator(Arguments args, String schemaFile, PrintStream output) {
    super(args, output);
    schemaPath = schemaFile == null ? args.inputFile("schema", "Schema file") :
      args.inputFile("schema", "Schema file", Path.of(schemaFile));
  }

  public static void main(String[] args) {
    // let users run `verify schema.yml` as a shortcut
    String schemaFile = null;
    if (args.length > 0 && args[0].endsWith(".yml") && !args[0].startsWith("-")) {
      schemaFile = args[0];
      args = Stream.of(args).skip(1).toArray(String[]::new);
    }
    var arguments = Arguments.fromEnvOrArgs(args);
    new SchemaValidator(arguments, schemaFile, System.out).runOrWatch();
  }


  /**
   * Returns the result of validating the profile defined by {@code schema} against the examples in
   * {@code specification}.
   */
  public static Result validate(SchemaConfig schema, SchemaSpecification specification) {
    var context = Contexts.buildRootContext(Arguments.of().silence(), schema.args());
    return validate(new ConfiguredProfile(schema, context), specification, context.config());
  }

  @Override
  protected Result validate(Set<Path> pathsToWatch) {
    Result result = null;
    try {
      pathsToWatch.add(schemaPath);
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
    }
    return result;
  }
}
