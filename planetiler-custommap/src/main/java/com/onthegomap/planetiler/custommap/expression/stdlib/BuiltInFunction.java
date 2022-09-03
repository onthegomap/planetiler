package com.onthegomap.planetiler.custommap.expression.stdlib;

import com.google.api.expr.v1alpha1.Decl;
import java.util.List;
import org.projectnessie.cel.interpreter.functions.Overload;

/**
 * Groups together a built-in function's type signature and implementation.
 */
record BuiltInFunction(Decl signature, List<Overload> implementations) {
  BuiltInFunction(Decl signature, Overload... implementations) {
    this(signature, List.of(implementations));
  }
}
