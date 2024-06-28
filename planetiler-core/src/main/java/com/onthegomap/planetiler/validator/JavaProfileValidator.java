package com.onthegomap.planetiler.validator;

import com.fasterxml.jackson.core.JacksonException;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.AnsiColors;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.Set;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.snakeyaml.engine.v2.exceptions.YamlEngineException;

/**
 * Validates a java profile against a yaml set of example source features and the vector tile features they should map
 * to.
 */
public class JavaProfileValidator extends BaseSchemaValidator {

  private final Profile profile;
  private final Path specPath;
  private final PlanetilerConfig config;

  JavaProfileValidator(PlanetilerConfig config, Path specPath, Profile profile, PrintStream output) {
    super(config.arguments(), output);
    this.config = config;
    this.profile = profile;
    this.specPath = specPath;
  }

  /**
   * Validates that {@code profile} maps input features to expected output features as defined in {@code specPath} and
   * returns true if successful, false if failed.
   */
  public static boolean validate(Profile profile, Path specPath, PlanetilerConfig config) {
    return new JavaProfileValidator(config, specPath, profile, System.out).runOrWatch();
  }

  @Override
  protected Result validate(Set<Path> pathsToWatch) {
    Result result = null;
    try {
      SchemaSpecification spec;
      pathsToWatch.add(specPath);
      spec = SchemaSpecification.load(specPath);
      result = validate(profile, spec, config);
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
