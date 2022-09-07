package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.custommap.expression.stdlib.PlanetilerStdLib;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.projectnessie.cel.extension.StringsLib;
import org.projectnessie.cel.tools.Script;
import org.projectnessie.cel.tools.ScriptCreateException;
import org.projectnessie.cel.tools.ScriptException;
import org.projectnessie.cel.tools.ScriptHost;

public class ConfigExpression<I extends ScriptContext, O> implements Function<I, O> {
  private static final Pattern EXPRESSION_PATTERN = Pattern.compile("^\\s*\\$\\{(.*)}\\s*$");
  private final Script script;
  private final Class<O> returnType;

  private ConfigExpression(Script script, Class<O> returnType) {
    this.script = script;
    this.returnType = returnType;
  }

  /** Returns true if this is a string expression like {@code "${ ... }"} */
  public static boolean isExpression(Object obj) {
    if (obj instanceof String string) {
      var matcher = EXPRESSION_PATTERN.matcher(string);
      return matcher.matches();
    }
    return false;
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

      return new ConfigExpression<>(script, expected);
    } catch (ScriptCreateException e) {
      throw new ParseException("Invalid expression", e);
    }
  }

  public O evaluate(I context) {
    try {
      return script.execute(returnType, context);
    } catch (ScriptException e) {
      throw new EvaluationException("Error evaluating script", e);
    }
  }

  @Override
  public O apply(I input) {
    return evaluate(input);
  }
}
