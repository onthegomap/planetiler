package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.custommap.TypeConversion;
import com.onthegomap.planetiler.custommap.expression.stdlib.PlanetilerStdLib;
import com.onthegomap.planetiler.util.Memoized;
import com.onthegomap.planetiler.util.Try;
import java.util.Objects;
import java.util.regex.Pattern;
import org.projectnessie.cel.extension.StringsLib;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;

/**
 * An expression that returns the result of evaluating a user-defined string script on the input environment context.
 *
 * @param <I> Type of the context that the script is expecting
 * @param <O> Result type of the script
 */
public class ConfigExpressionScript<I extends ScriptContext, O> implements ConfigExpression<I, O> {
  private static final Pattern EXPRESSION_PATTERN = Pattern.compile("^\\s*\\$\\{(.*)}\\s*$");
  private static final Pattern ESCAPED_EXPRESSION_PATTERN = Pattern.compile("^\\s*\\\\+\\$\\{(.*)}\\s*$");
  private static final Memoized<ConfigExpressionScript<?, ?>, ?> staticEvaluationCache =
    Memoized.memoize(ConfigExpressionScript::doStaticEvaluate);
  private final Script script;
  private final Class<O> returnType;
  private final String scriptText;
  private final ScriptEnvironment<I> descriptor;

  private ConfigExpressionScript(String scriptText, Script script, ScriptEnvironment<I> descriptor,
    Class<O> returnType) {
    this.scriptText = scriptText;
    this.script = script;
    this.returnType = returnType;
    this.descriptor = descriptor;
  }

  /** Returns true if this is a string expression like {@code "${ ... }"} */
  public static boolean isScript(Object obj) {
    if (obj instanceof String string) {
      var matcher = EXPRESSION_PATTERN.matcher(string);
      return matcher.matches();
    }
    return false;
  }

  /**
   * Returns true if this is an escaped string expression that should just be treated as a string like {@code "\${ ...
   * }"}
   */
  public static boolean isEscapedScript(Object obj) {
    if (obj instanceof String string) {
      var matcher = ESCAPED_EXPRESSION_PATTERN.matcher(string);
      return matcher.matches();
    }
    return false;
  }

  /**
   * Removes script escape character from a string {@code "\${ ... }"} becomes {@code "${ ... }"}
   */
  public static Object unescape(Object obj) {
    if (isEscapedScript(obj)) {
      return obj.toString().replaceFirst("\\\\\\$", "\\$");
    } else {
      return obj;
    }
  }

  /**
   * Returns the script text between the {@code "${ ... }"} characters.
   */
  public static String extractScript(Object obj) {
    if (obj instanceof String string) {
      var matcher = EXPRESSION_PATTERN.matcher(string);
      if (matcher.matches()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  /**
   * Returns an expression parsed from a user-supplied script string.
   *
   * @throws ParseException if the script failes to compile or type-check
   */
  public static <I extends ScriptContext> ConfigExpressionScript<I, Object> parse(String string,
    ScriptEnvironment<I> description) {
    return parse(string, description, Object.class);
  }

  /**
   * Returns an expression parsed from a user-supplied script string that coerces the result to {@code O}.
   *
   * @throws ParseException if the script failes to compile or type-check
   */
  public static <I extends ScriptContext, O> ConfigExpressionScript<I, O> parse(String string,
    ScriptEnvironment<I> description, Class<O> expected) {
    ScriptHost scriptHost = ScriptHost.newBuilder().build();
    try {
      var scriptBuilder = scriptHost.buildScript(string).withLibraries(
        new StringsLib(),
        new PlanetilerStdLib()
      );
      if (!description.declarations().isEmpty()) {
        scriptBuilder.withDeclarations(description.declarations());
      }
      var script = scriptBuilder.build();

      return new ConfigExpressionScript<>(string, script, description, expected);
    } catch (ScriptCreateException e) {
      throw new ParseException(string, e);
    }
  }

  @Override
  public O apply(I input) {
    try {
      return TypeConversion.convert(script.execute(Object.class, input), returnType);
    } catch (ScriptException e) {
      throw new EvaluationException("Error evaluating script '%s'".formatted(scriptText), e);
    }
  }

  @Override
  public boolean equals(Object o) {
    // ignore the parsed script object
    return this == o || (o instanceof ConfigExpressionScript<?, ?> config &&
      returnType.equals(config.returnType) &&
      scriptText.equals(config.scriptText) &&
      descriptor.equals(config.descriptor));
  }

  @Override
  public int hashCode() {
    // ignore the parsed script object
    return Objects.hash(returnType, scriptText, descriptor);
  }

  /**
   * Attempts to parse and evaluate this script in an environment with no variables.
   * <p>
   * If this returns {@link Try.Success} then it means this script will always return the same constant value and we can
   * avoid evaluating it at runtime.
   */
  public Try<O> tryStaticEvaluate() {
    // type checking can be expensive when run hundreds of times simplifying expressions iteratively and it never
    // changes for a given script and input environment, so cache results between calls.
    return staticEvaluationCache.tryApply(this, returnType);
  }

  private O doStaticEvaluate() {
    return ConfigExpressionScript.parse(scriptText, descriptor.root().description(), returnType)
      .apply(descriptor.root());
  }

  @Override
  public String toString() {
    return "ConfigExpression[returnType=" + returnType +
      ", scriptText='" + scriptText + '\'' +
      ']';
  }

  @Override
  public ConfigExpression<I, O> simplifyOnce() {
    var result = tryStaticEvaluate();
    if (result.isSuccess()) {
      return ConfigExpression.constOf(result.get());
    } else if (descriptor.containsVariable(scriptText.strip())) {
      return ConfigExpression.variable(ConfigExpression.signature(descriptor, returnType), scriptText.strip());
    }
    return this;
  }

  @Override
  public ScriptEnvironment<I> environment() {
    return descriptor;
  }
}
