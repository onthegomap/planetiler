package com.onthegomap.planetiler.custommap.expression.stdlib;

import static org.projectnessie.cel.checker.Decls.newOverload;

import com.google.api.expr.v1alpha1.Type;
import java.util.List;
import java.util.Objects;
import java.util.function.DoubleBinaryOperator;
import java.util.function.LongBinaryOperator;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.BoolT;
import org.projectnessie.cel.common.types.DoubleT;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.IntT;
import org.projectnessie.cel.common.types.NullT;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.common.types.ref.Val;
import org.projectnessie.cel.common.types.traits.Lister;
import org.projectnessie.cel.common.types.traits.Mapper;
import org.projectnessie.cel.interpreter.functions.Overload;

/**
 * Built-in functions to expose to all dynamic expression used in planetiler configs.
 */
public class PlanetilerStdLib extends PlanetilerLib {
  private static final int VARARG_LIMIT = 32;
  private static final Type T = Decls.newTypeParamType("T");
  private static final Type K = Decls.newTypeParamType("K");
  private static final Type V = Decls.newTypeParamType("V");

  public PlanetilerStdLib() {
    super(List.of(
      // coalesce(a, b, c...) -> first non-null value
      new BuiltInFunction(
        Decls.newFunction("coalesce",
          IntStream.range(0, VARARG_LIMIT)
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
      ),

      // map.has(key) -> true if key is present in map
      // map.has(key, value...) true if the value for key is in the list of values provided
      new BuiltInFunction(
        Decls.newFunction("has",
          IntStream.range(0, VARARG_LIMIT)
            .mapToObj(
              i -> Decls.newInstanceOverload("map_has_" + i, Stream.concat(
                Stream.of(Decls.newMapType(K, V), K),
                IntStream.range(0, i).mapToObj(n -> V)
              ).toList(), Decls.Bool)
            ).toList()
        ),
        Overload.overload("has",
          null,
          null,
          (map, key) -> {
            try {
              return getFromMap(map, key) != null ? BoolT.True : BoolT.False;
            } catch (RuntimeException e) {
              return Err.newErr(e, "%s", e.getMessage());
            }
          },
          args -> {
            try {
              Val elem = getFromMap(args[0], args[1]);
              if (elem == null) {
                return BoolT.False;
              }
              for (int i = 2; i < args.length; i++) {
                if (args[i].equals(elem)) {
                  return BoolT.True;
                }
              }
              return BoolT.False;
            } catch (RuntimeException e) {
              return Err.newErr(e, "%s", e.getMessage());
            }
          })
      ),

      // map.get(key) -> the value for key, or null if missing
      new BuiltInFunction(
        Decls.newFunction("get", Decls.newInstanceOverload("get", List.of(Decls.newMapType(K, V), K), V)),
        Overload.binary("get", (map, key) -> {
          try {
            var value = getFromMap(map, key);
            return value == null ? NullT.NullValue : value;
          } catch (RuntimeException e) {
            return Err.newErr(e, "%s", e.getMessage());
          }
        })
      ),

      // map.getOrDefault(key, default) -> the value for key, or default if missing
      new BuiltInFunction(
        Decls.newFunction("getOrDefault",
          Decls.newInstanceOverload("getOrDefault", List.of(Decls.newMapType(K, V), K, V), V)),
        Overload.function("getOrDefault", args -> {
          try {
            var value = getFromMap(args[0], args[1]);
            return value == null ? args[2] : value;
          } catch (RuntimeException e) {
            return Err.newErr(e, "%s", e.getMessage());
          }
        })
      ),

      // min(list) -> the minimum value from the list, or null if empty
      new BuiltInFunction(
        Decls.newFunction("min",
          Decls.newOverload("min_int", List.of(Decls.newListType(Decls.Int)), Decls.Int),
          Decls.newOverload("min_double", List.of(Decls.newListType(Decls.Double)), Decls.Double)
        ),
        Overload.unary("min", list -> reduceNumeric(list, Math::min, Math::min))
      ),

      // max(list) -> the maximum value from the list, or null if empty
      new BuiltInFunction(
        Decls.newFunction("max",
          Decls.newOverload("max_int", List.of(Decls.newListType(Decls.Int)), Decls.Int),
          Decls.newOverload("max_double", List.of(Decls.newListType(Decls.Double)), Decls.Double)
        ),
        Overload.unary("max", list -> reduceNumeric(list, Math::max, Math::max))
      )
    ));
  }

  private static Val getFromMap(Val map, Val key) {
    return map instanceof Mapper mapper ? mapper.find(key) : null;
  }

  private static Val reduceNumeric(Val list, LongBinaryOperator intFn, DoubleBinaryOperator doubleFn) {
    try {
      var iterator = ((Lister) list).iterator();
      if (!iterator.hasNext().booleanValue()) {
        return NullT.NullValue;
      }
      var next = iterator.next();
      if (next instanceof IntT intT) {
        long acc = intT.intValue();
        while (iterator.hasNext().booleanValue()) {
          acc = intFn.applyAsLong(iterator.next().convertToNative(Long.class), acc);
        }
        return IntT.intOf(acc);
      } else if (next instanceof DoubleT doubleT) {
        double acc = doubleT.convertToNative(Double.class);
        while (iterator.hasNext().booleanValue()) {
          acc = doubleFn.applyAsDouble(iterator.next().convertToNative(Double.class), acc);
        }
        return DoubleT.doubleOf(acc);
      } else {
        return Err.newErr("Bad element of list for min(): %s", next);
      }
    } catch (RuntimeException e) {
      return Err.newErr(e, "%s", e.getMessage());
    }
  }
}
