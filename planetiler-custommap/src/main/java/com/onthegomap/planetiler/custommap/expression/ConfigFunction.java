package com.onthegomap.planetiler.custommap.expression;

import com.onthegomap.planetiler.custommap.TypeConversion;
import com.onthegomap.planetiler.expression.Expression;
import com.onthegomap.planetiler.expression.MultiExpression;
import com.onthegomap.planetiler.expression.Simplifiable;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Stream;

public interface ConfigFunction<I extends ScriptContext, O> extends Function<I, O>, Simplifiable<ConfigFunction<I, O>> {

  static <I extends ScriptContext, O> ConfigFunction<I, O> expression(Signature<I, O> signature, String text) {
    return ConfigExpression.parse(text, signature.in(), signature.out());
  }

  static <I extends ScriptContext, O> ConfigFunction<I, O> variable(Signature<I, O> signature, String text) {
    return new Variable<>(signature, text);
  }

  static <I extends ScriptContext, O> ConfigFunction<I, O> constOf(O value) {
    return new Const<>(value);
  }

  static <I extends ScriptContext, O> ConfigFunction<I, O> coalesce(
    List<ConfigFunction<I, O>> values) {
    return new Coalesce<>(values);
  }

  static <I extends ScriptContext, O> Match<I, O> match(Signature<I, O> description,
    MultiExpression<ConfigFunction<I, O>> multiExpression) {
    return new Match<>(description, multiExpression, constOf(null));
  }

  static <I extends ScriptContext, O> Match<I, O> match(Signature<I, O> description,
    MultiExpression<ConfigFunction<I, O>> multiExpression, ConfigFunction<I, O> fallback) {
    return new Match<>(description, multiExpression, fallback);
  }

  record Const<I extends ScriptContext, O> (O value) implements ConfigFunction<I, O> {
    @Override
    public O apply(I i) {
      return value;
    }
  }

  record Match<I extends ScriptContext, O> (
    Signature<I, O> signature,
    MultiExpression<ConfigFunction<I, O>> multiExpression,
    ConfigFunction<I, O> fallback,
    MultiExpression.Index<ConfigFunction<I, O>> indexed
  ) implements ConfigFunction<I, O> {
    public Match(
      Signature<I, O> signature,
      MultiExpression<ConfigFunction<I, O>> multiExpression,
      ConfigFunction<I, O> fallback
    ) {
      this(signature, multiExpression, fallback, multiExpression.index());
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        (o instanceof Match<?, ?> match &&
          Objects.equals(signature, match.signature) &&
          Objects.equals(multiExpression, match.multiExpression) &&
          Objects.equals(fallback, match.fallback));
    }

    @Override
    public int hashCode() {
      return Objects.hash(signature, multiExpression, fallback);
    }

    @Override
    public O apply(I i) {
      var resultFunction = indexed.getOrElse(i, fallback);
      return resultFunction == null ? null : resultFunction.apply(i);
    }

    @Override
    public ConfigFunction<I, O> simplifyOnce() {
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

    public Match<I, O> withDefaultValue(ConfigFunction<I, O> newFallback) {
      return new Match<>(signature, multiExpression, newFallback);
    }
  }

  record Coalesce<I extends ScriptContext, O> (List<? extends ConfigFunction<I, O>> children)
    implements ConfigFunction<I, O> {

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
    public ConfigFunction<I, O> simplifyOnce() {
      return switch (children.size()) {
        case 0 -> constOf(null);
        case 1 -> children.get(0);
        default -> {
          var result = children.stream()
            .flatMap(
              child -> child instanceof Coalesce<I, O> childCoalesce ? childCoalesce.children.stream() :
                Stream.of(child))
            .filter(child -> !child.equals(constOf(null)))
            .distinct()
            .toList();
          var indexOfFirstConst = result.stream().takeWhile(d -> !(d instanceof ConfigFunction.Const<I, O>)).count();
          yield coalesce(result.stream().limit(indexOfFirstConst + 1).toList());
        }
      };
    }
  }

  record Variable<I extends ScriptContext, O> (
    Signature<I, O> signature,
    String name
  ) implements ConfigFunction<I, O> {

    public Variable {
      if (!signature.in.containsVariable(name)) {
        throw new IllegalArgumentException("Variable not available: " + name);
      }
    }

    @Override
    public O apply(I i) {
      return TypeConversion.convert(i.apply(name), signature.out);
    }
  }

  static <I extends ScriptContext, O> Signature<I, O> signature(ScriptContextDescription<I> in, Class<O> out) {
    return new Signature<>(in, out);
  }

  record Signature<I extends ScriptContext, O> (ScriptContextDescription<I> in, Class<O> out) {
    public <O2> Signature<I, O2> withOutput(Class<O2> newOut) {
      return new Signature<>(in, newOut);
    }
  }
}
