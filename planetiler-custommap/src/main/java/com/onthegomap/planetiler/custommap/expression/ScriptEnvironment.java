package com.onthegomap.planetiler.custommap.expression;

import com.google.api.expr.v1alpha1.Decl;
import java.util.List;
import java.util.stream.Stream;

/**
 * Type definitions for the environment that a script expression runs in.
 *
 * @param types        Additional types available.
 * @param declarations Global variable types
 * @param clazz        Class of the input context type
 * @param <T>          The runtime expression context type
 */
public record ScriptEnvironment<T extends ScriptContext> (List<Object> types, List<Decl> declarations, Class<T> clazz) {
  private static <T> List<T> concat(List<T> a, T[] b) {
    return Stream.concat(a.stream(), Stream.of(b)).toList();
  }

  /** Returns a copy of this environment with a new input type {@code U}. */
  public <U extends ScriptContext> ScriptEnvironment<U> forInput(Class<U> newClazz) {
    return new ScriptEnvironment<>(types, declarations, newClazz);
  }

  /** Returns a copy of this environment with a list of variable declarations appended to the global environment. */
  public ScriptEnvironment<T> withDeclarations(Decl... others) {
    return new ScriptEnvironment<>(types, concat(declarations, others), clazz);
  }

  /** Returns an empty environment with no variables defined. */
  public static ScriptEnvironment<ScriptContext> root() {
    return new ScriptEnvironment<>(List.of(), List.of(), ScriptContext.class);
  }

  /** Returns true if this contains a variable declaration for {@code variable}. */
  public boolean containsVariable(String variable) {
    return declarations().stream().anyMatch(decl -> decl.getName().equals(variable));
  }

  @Override
  public String toString() {
    return "ScriptContextDescription{" +
      "declarations=" + declarations.stream().map(Decl::getName).toList() +
      ", clazz=" + clazz +
      '}';
  }
}
