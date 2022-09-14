package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.custommap.TypeConversion;
import com.onthegomap.planetiler.custommap.expression.stdlib.PlanetilerStdLib;
import com.onthegomap.planetiler.util.Try;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;
import org.projectnessie.cel.extension.StringsLib;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;

public class ConfigExpression<I extends ScriptContext, O> implements ConfigFunction<I, O> {
  private static final Pattern EXPRESSION_PATTERN = Pattern.compile("^\\s*\\$\\{(.*)}\\s*$");
  private static final Pattern ESCAPED_EXPRESSION_PATTERN = Pattern.compile("^\\s*\\\\+\\$\\{(.*)}\\s*$");
  private final Script script;
  private final Class<O> returnType;
  private final String scriptText;
  private final ScriptContextDescription<I> descriptor;

  private ConfigExpression(String scriptText, Script script, ScriptContextDescription<I> descriptor,
    Class<O> returnType) {
    this.scriptText = scriptText;
    this.script = script;
    this.returnType = returnType;
    this.descriptor = descriptor;
  }

  /** Returns true if this is a string expression like {@code "${ ... }"} */
  public static boolean isExpression(Object obj) {
    if (obj instanceof String string) {
      var matcher = EXPRESSION_PATTERN.matcher(string);
      return matcher.matches();
    }
    return false;
  }

  public static boolean isEscapedExpression(Object obj) {
    if (obj instanceof String string) {
      var matcher = ESCAPED_EXPRESSION_PATTERN.matcher(string);
      return matcher.matches();
    }
    return false;
  }

  public static Object unescapeExpression(Object obj) {
    if (isEscapedExpression(obj)) {
      return obj.toString().replaceFirst("\\\\\\$", "\\$");
    } else {
      return obj;
    }
  }

  public static String extractFromEscaped(Object obj) {
    if (obj instanceof String string) {
      var matcher = EXPRESSION_PATTERN.matcher(string);
      if (matcher.matches()) {
        return matcher.group(1);
      }
    }
    return null;
  }

  public static <I extends ScriptContext> ConfigExpression<I, Object> parse(String string,
    ScriptContextDescription<I> description) {
    return parse(string, description, Object.class);
  }

  public static <I extends ScriptContext, O> ConfigExpression<I, O> parse(String string,
    ScriptContextDescription<I> description, Class<O> expected) {
    ScriptHost scriptHost = ScriptHost.newBuilder().build();
    try {
      var scriptBuilder = scriptHost.buildScript(string).withLibraries(
        new StringsLib(),
        new PlanetilerStdLib()
      );
      if (!description.declarations().isEmpty()) {
        scriptBuilder.withDeclarations(description.declarations());
      }
      if (!description.types().isEmpty()) {
        scriptBuilder.withTypes(description.types());
      }
      var script = scriptBuilder.build();

      return new ConfigExpression<>(string, script, description, expected);
    } catch (ScriptCreateException e) {
      throw new ParseException("Invalid expression", e);
    }
  }

  @Override
  public O apply(I input) {
    try {
      return TypeConversion.convert(script.execute(Object.class, input), returnType);
    } catch (ScriptException e) {
      throw new EvaluationException("Error evaluating script", e);
    }
  }

  @Override
  public boolean equals(Object o) {
    return this == o || (o instanceof ConfigExpression<?, ?> config &&
      returnType.equals(config.returnType) &&
      scriptText.equals(config.scriptText));
  }

  @Override
  public int hashCode() {
    return Objects.hash(returnType, scriptText);
  }

  private static final Map<ConfigExpression<?, ?>, Boolean> staticEvaluationCache = new ConcurrentHashMap<>();

  public Try<O> tryStaticEvaluate() {
    boolean canStaticEvaluate =
      staticEvaluationCache.computeIfAbsent(this, config -> config.doTryStaticEvaluate().isSuccess());
    if (canStaticEvaluate) {
      return doTryStaticEvaluate();
    } else {
      return Try.failure(new IllegalStateException());
    }
  }

  private Try<O> doTryStaticEvaluate() {
    return Try
      .apply(() -> ConfigExpression.parse(scriptText, Contexts.Root.DESCRIPTION, returnType).apply(Contexts.root()));
  }

  @Override
  public String toString() {
    return "ConfigExpression[returnType=" + returnType +
      ", scriptText='" + scriptText + '\'' +
      ']';
  }

  @Override
  public ConfigFunction<I, O> simplifyOnce() {
    var result = tryStaticEvaluate();
    if (result.isSuccess()) {
      return ConfigFunction.constOf(result.item());
    } else if (descriptor.containsVariable(scriptText.strip())) {
      return ConfigFunction.variable(ConfigFunction.signature(descriptor, returnType), scriptText.strip());
    }
    return this;
  }
}
