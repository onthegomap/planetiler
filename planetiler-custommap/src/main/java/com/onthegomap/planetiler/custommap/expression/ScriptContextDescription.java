package com.onthegomap.planetiler.custommap.expression;

import com.google.api.expr.v1alpha1.Decl;
import java.util.List;
import java.util.stream.Stream;

/**
 * Type definitions for the environment that a CEL expression runs in.
 *
 * @param types        Additional types available.
 * @param declarations Global variable types
 * @param <T>          The runtime expression context type
 */
public record ScriptContextDescription<T extends ScriptContext> (List<Object> types, List<Decl> declarations) {
  private static <T> List<T> concat(List<T> a, T[] b) {
    return Stream.concat(a.stream(), Stream.of(b)).toList();
  }

  public <U extends ScriptContext> ScriptContextDescription<U> withTypes(Object... others) {
    return new ScriptContextDescription<>(concat(types, others), declarations);
  }

  public <U extends ScriptContext> ScriptContextDescription<U> withDeclarations(Decl... others) {
    return new ScriptContextDescription<>(types, concat(declarations, others));
  }

  public static <T extends ScriptContext> ScriptContextDescription<T> root() {
    return new ScriptContextDescription<>(List.of(), List.of());
  }
}
