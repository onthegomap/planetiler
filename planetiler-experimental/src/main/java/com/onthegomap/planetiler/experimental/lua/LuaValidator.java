package com.onthegomap.planetiler.experimental.lua;

import com.fasterxml.jackson.core.JacksonException;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.validator.BaseSchemaValidator;
import com.onthegomap.planetiler.validator.SchemaSpecification;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;

/**
 * Validates a lua profile against a yaml set of example source features and the vector tile features they should map to
 **/
public class LuaValidator extends BaseSchemaValidator {

  private final Path scriptPath;

  LuaValidator(Arguments args, String schemaFile, PrintStream output) {
    super(args, output);
    scriptPath = schemaFile == null ? args.inputFile("script", "Schema file") :
      args.inputFile("script", "Script file", Path.of(schemaFile));
  }

  public static void main(String[] args) {
    // let users run `verify schema.lua` as a shortcut
    String schemaFile = null;
    if (args.length > 0 && args[0].endsWith(".lua") && !args[0].startsWith("-")) {
      schemaFile = args[0];
      args = Stream.of(args).skip(1).toArray(String[]::new);
    }
    var arguments = Arguments.fromEnvOrArgs(args).silence();
    new LuaValidator(arguments, schemaFile, System.out).runOrWatch();
  }

  @Override
  protected Result validate(Set<Path> pathsToWatch) {
    Result result = null;
    try {
      pathsToWatch.add(scriptPath);
      var env = LuaEnvironment.loadScript(args, scriptPath, pathsToWatch);
      // examples can either be embedded in the lua file, or referenced
      Path specPath;
      if (env.planetiler.examples != null) {
        specPath = Path.of(env.planetiler.examples);
        if (!specPath.isAbsolute()) {
          specPath = scriptPath.resolveSibling(specPath);
        }
      } else {
        specPath = args.file("spec", "yaml spec", null);
      }
      SchemaSpecification spec;
      if (specPath != null) {
        pathsToWatch.add(specPath);
        spec = SchemaSpecification.load(specPath);
      } else {
        spec = new SchemaSpecification(List.of());
      }
      result = validate(env.profile, spec, PlanetilerConfig.from(args));
    } catch (Exception exception) {
      Throwable rootCause = ExceptionUtils.getRootCause(exception);
      if (hasCause(exception, YamlEngineException.class) || hasCause(exception, JacksonException.class)) {
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
