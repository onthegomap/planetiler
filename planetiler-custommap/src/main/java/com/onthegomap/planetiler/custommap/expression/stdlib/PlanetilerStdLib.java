package com.onthegomap.planetiler.custommap.expression.stdlib;

import static org.projectnessie.cel.checker.Decls.newOverload;

import com.google.api.expr.v1alpha1.Type;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.NullT;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.interpreter.functions.Overload;

/**
 * Built-in functions to expose to all CEL expression used in planetiler configs.
 */
public class PlanetilerStdLib extends PlanetilerLib {
  private static final Type T = Decls.newTypeParamType("T");

  public PlanetilerStdLib() {
    super(List.of(
      // coalesce(a, b, c...) -> first non-null value
      new BuiltInFunction(
        Decls.newFunction("coalesce",
          IntStream.range(0, 22)
            .mapToObj(
              i -> newOverload("coalesce_" + i, IntStream.range(0, i).mapToObj(d -> Decls.Any).toList(), Decls.Any))
            .toList()
        ),
        Overload.overload("coalesce",
          null,
          null,
          (a, b) -> a == null || a instanceof NullT ? b : a,
          args -> {
            for (var arg : args) {
              if (!(arg instanceof NullT)) {
                return arg;
              }
            }
            return NullT.NullValue;
          })
      ),

      // nullif(a, b) -> null if a == b, otherwise a
      new BuiltInFunction(
        Decls.newFunction("nullif",
          Decls.newOverload("nullif", List.of(T, T), T)
        ),
        Overload.binary("nullif", (a, b) -> Objects.equals(a, b) ? NullT.NullValue : a)
      ),

      // string.regexp_replace(regex, replacement) -> replaces all matches for regex in string with replacement
      new BuiltInFunction(
        Decls.newFunction("regexp_replace",
          Decls.newInstanceOverload("regexp_replace", List.of(Decls.String, Decls.String, Decls.String), Decls.String)
        ),
        Overload.function("regexp_replace", values -> {
          try {
            String string = ((String) values[0].value());
            String regexp = ((String) values[1].value());
            String replace = ((String) values[2].value());
            return StringT.stringOf(string.replaceAll(regexp, replace));
          } catch (RuntimeException e) {
            return Err.newErr(e, "%s", e.getMessage());
          }
        })
      )
    ));
  }
}
