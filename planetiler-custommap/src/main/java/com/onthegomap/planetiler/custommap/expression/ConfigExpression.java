package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.custommap.TypeConversion;
import com.onthegomap.planetiler.expression.DataType;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.Simplifiable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * A function defined in part of a schema config that produces an output value (min zoom, attribute value, etc.) for a
 * feature at runtime.
 * <p>
 * This can be parsed from a structured object that lists combinations of tag key/values, an embedded script, or a
 * combination of the two.
 *
 * @param <I> Type of the input context that expressions can pull values from at runtime.
 * @param <O> Output type
 */
public interface ConfigExpression<I extends ScriptContext, O>
  extends Function<I, O>, Simplifiable<ConfigExpression<I, O>> {

  static <I extends ScriptContext, O> ConfigExpression<I, O> script(Signature<I, O> signature, String script) {
    return ConfigExpressionScript.parse(script, signature.in(), signature.out());
  }

  static <I extends ScriptContext, O> ConfigExpression<I, O> variable(Signature<I, O> signature, String text) {
    return new Variable<>(signature, text);
  }

  static <I extends ScriptContext, O> ConfigExpression<I, O> constOf(O value) {
    return new Const<>(value);
  }

  static <I extends ScriptContext, O> ConfigExpression<I, O> coalesce(
    List<ConfigExpression<I, O>> values) {
    return new Coalesce<>(values);
  }

  static <I extends ScriptContext, O> ConfigExpression<I, O> getTag(Signature<I, O> signature,
    ConfigExpression<I, String> tag) {
    return new GetTag<>(signature, tag);
  }

  static <I extends ScriptContext, O> ConfigExpression<I, O> getArg(Signature<I, O> signature,
    ConfigExpression<I, String> tag) {
    return new GetArg<>(signature, tag);
  }

  static <I extends ScriptContext, O> ConfigExpression<I, O> cast(Signature<I, O> signature,
    ConfigExpression<I, ?> input, DataType dataType) {
    return new Cast<>(signature, input, dataType);
  }

  static <I extends ScriptContext, O> Match<I, O> match(Signature<I, O> description,
    MultiExpression<ConfigExpression<I, O>> multiExpression) {
    return new Match<>(description, multiExpression, constOf(null));
  }

  static <I extends ScriptContext, O> Match<I, O> match(Signature<I, O> description,
    MultiExpression<ConfigExpression<I, O>> multiExpression, ConfigExpression<I, O> fallback) {
    return new Match<>(description, multiExpression, fallback);
  }

  static <I extends ScriptContext, O> Signature<I, O> signature(ScriptEnvironment<I> in, Class<O> out) {
    return new Signature<>(in, out);
  }

  /** An expression that always returns {@code value}. */
  record Const<I extends ScriptContext, O>(O value) implements ConfigExpression<I, O> {

    @Override
    public O apply(I i) {
      return value;
    }
  }

  /** An expression that returns the value associated with the first matching boolean expression. */
  record Match<I extends ScriptContext, O>(
    Signature<I, O> signature,
    MultiExpression<ConfigExpression<I, O>> multiExpression,
    ConfigExpression<I, O> fallback,
    MultiExpression.Index<ConfigExpression<I, O>> indexed
  ) implements ConfigExpression<I, O> {

    public Match(
      Signature<I, O> signature,
      MultiExpression<ConfigExpression<I, O>> multiExpression,
      ConfigExpression<I, O> fallback
    ) {
      this(signature, multiExpression, fallback, multiExpression.index());
    }

    @Override
    public boolean equals(Object o) {
      // ignore the indexed expression
      return this == o ||
        (o instanceof Match<?, ?> match &&
          Objects.equals(signature, match.signature) &&
          Objects.equals(multiExpression, match.multiExpression) &&
          Objects.equals(fallback, match.fallback));
    }

    @Override
    public int hashCode() {
      // ignore the indexed expression
      return Objects.hash(signature, multiExpression, fallback);
    }

    @Override
    public O apply(I i) {
      var resultFunction = indexed.getOrElse(i, fallback);
      return resultFunction == null ? null : resultFunction.apply(i);
    }

    @Override
    public ConfigExpression<I, O> simplifyOnce() {
      var newMultiExpression = multiExpression
        .mapResults(Simplifiable::simplifyOnce)
        .simplify();
      var newFallback = fallback.simplifyOnce();
      if (newMultiExpression.expressions().isEmpty()) {
        return newFallback;
      }
      var expressions = newMultiExpression.expressions();
      for (int i = 0; i < expressions.size(); i++) {
        var expression = expressions.get(i);
        // if one of the cases is always true, then ignore the cases after it and make this value the fallback
        if (Expression.TRUE.equals(expression.expression())) {
          return new Match<>(
            signature,
            MultiExpression.of(expressions.stream().limit(i).toList()),
            expression.result()
          );
        }
      }
      return new Match<>(signature, newMultiExpression, newFallback);
    }

    public Match<I, O> withDefaultValue(ConfigExpression<I, O> newFallback) {
      return new Match<>(signature, multiExpression, newFallback);
    }
  }

  /** An expression that returns the first non-null result of evaluating each child expression. */
  record Coalesce<I extends ScriptContext, O>(List<? extends ConfigExpression<I, O>> children)
    implements ConfigExpression<I, O> {

    @Override
    public O apply(I i) {
      for (var condition : children) {
        var result = condition.apply(i);
        if (result != null) {
          return result;
        }
      }
      return null;
    }

    @Override
    public ConfigExpression<I, O> simplifyOnce() {
      return switch (children.size()) {
        case 0 -> constOf(null);
        case 1 -> children.getFirst();
        default -> {
          var result = children.stream()
            .flatMap(
              child -> child instanceof Coalesce<?, ?> childCoalesce ? childCoalesce.children.stream() :
                Stream.of(child))
            .filter(child -> !child.equals(constOf(null)))
            .distinct()
            .toList();
          var indexOfFirstConst = result.stream().takeWhile(d -> !(d instanceof ConfigExpression.Const<?, ?>)).count();
          yield coalesce(result.stream().map(d -> {
            @SuppressWarnings("unchecked") ConfigExpression<I, O> casted = (ConfigExpression<I, O>) d;
            return casted;
          }).limit(indexOfFirstConst + 1).toList());
        }
      };
    }
  }

  /** An expression that returns the value associated a given variable name at runtime. */
  record Variable<I extends ScriptContext, O>(
    Signature<I, O> signature,
    String name
  ) implements ConfigExpression<I, O> {

    public Variable {
      if (!signature.in.containsVariable(name)) {
        throw new ParseException("Variable not available: " + name);
      }
    }

    @Override
    public O apply(I i) {
      return TypeConversion.convert(i.apply(name), signature.out);
    }
  }

  /** An expression that returns the value associated a given tag of the input feature at runtime. */
  record GetTag<I extends ScriptContext, O>(
    Signature<I, O> signature,
    ConfigExpression<I, String> tag
  ) implements ConfigExpression<I, O> {

    @Override
    public O apply(I i) {
      return TypeConversion.convert(i.tagValueProducer().valueForKey(i, tag.apply(i)), signature.out);
    }

    @Override
    public ConfigExpression<I, O> simplifyOnce() {
      return new GetTag<>(signature, tag.simplifyOnce());
    }
  }

  /** An expression that returns the value associated a given argument at runtime. */
  record GetArg<I extends ScriptContext, O>(
    Signature<I, O> signature,
    ConfigExpression<I, String> arg
  ) implements ConfigExpression<I, O> {

    @Override
    public O apply(I i) {
      return TypeConversion.convert(i.argument(arg.apply(i)), signature.out);
    }

    @Override
    public ConfigExpression<I, O> simplifyOnce() {
      var key = arg.simplifyOnce();
      if (key instanceof ConfigExpression.Const<I, String> constKey) {
        var rawResult = signature.in.root().argument(constKey.value);
        return constOf(TypeConversion.convert(rawResult, signature.out));
      } else {
        return new GetArg<>(signature, key);
      }
    }
  }

  /** An expression that converts the input to a desired output {@link DataType} at runtime. */
  record Cast<I extends ScriptContext, O>(
    Signature<I, O> signature,
    ConfigExpression<I, ?> input,
    DataType output
  ) implements ConfigExpression<I, O> {


    @Override
    public O apply(I i) {
      return TypeConversion.convert(output.convertFrom(input.apply(i)), signature.out);
    }

    @Override
    public ConfigExpression<I, O> simplifyOnce() {
      var in = input.simplifyOnce();
      if (in instanceof ConfigExpression.Const<?, ?> inConst) {
        return constOf(TypeConversion.convert(output.convertFrom(inConst.value), signature.out));
      } else if (in instanceof ConfigExpression.Cast<?, ?> cast && cast.output == output) {
        @SuppressWarnings("unchecked") ConfigExpression<I, ?> newIn = (ConfigExpression<I, ?>) cast.input;
        return cast(signature, newIn, output);
      } else {
        return new Cast<>(signature, input.simplifyOnce(), output);
      }
    }
  }

  record Signature<I extends ScriptContext, O>(ScriptEnvironment<I> in, Class<O> out) {

    public <O2> Signature<I, O2> withOutput(Class<O2> newOut) {
      return new Signature<>(in, newOut);
    }
  }
}
