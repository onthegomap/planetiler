package com.onthegomap.planetiler.custommap.expression;

import com.google.api.expr.v1alpha1.Decl;
import java.util.List;
import java.util.stream.Stream;

/**
 * Type definitions for the environment that a CEL expression runs in.
 *
 * @param types        Additional types available.
 * @param declarations Global variable types
 * @param clazz        Class of the input context type
 * @param <T>          The runtime expression context type
 */
public record ScriptContextDescription<T extends ScriptContext> (List<Object> types, List<Decl> declarations,
  Class<T> clazz) {
  private static <T> List<T> concat(List<T> a, T[] b) {
    return Stream.concat(a.stream(), Stream.of(b)).toList();
  }

  public <U extends ScriptContext> ScriptContextDescription<U> forInput(Class<U> newClazz) {
    return new ScriptContextDescription<>(types, declarations, newClazz);
  }

  public ScriptContextDescription<T> withDeclarations(Decl... others) {
    return new ScriptContextDescription<>(types, concat(declarations, others), clazz);
  }

  public static ScriptContextDescription<ScriptContext> root() {
    return new ScriptContextDescription<>(List.of(), List.of(), ScriptContext.class);
  }
}
