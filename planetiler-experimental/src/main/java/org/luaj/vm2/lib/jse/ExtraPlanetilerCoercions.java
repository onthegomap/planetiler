package org.luaj.vm2.lib.jse;

import static org.luaj.vm2.lib.jse.CoerceLuaToJava.SCORE_WRONG_TYPE;

import com.onthegomap.planetiler.experimental.lua.LuaConversions;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.ToIntFunction;
import org.luaj.vm2.LuaTables;
import org.luaj.vm2.LuaValue;

/**
 * Call {@link #install()} to install planetiler's extra type conversions: {@link List}/table
 */
public class ExtraPlanetilerCoercions {
  public static void install() {}

  static {
    coerceTable(List.class, LuaConversions::toJavaList);
    coerceTable(Collection.class, LuaConversions::toJavaCollection);
    coerceTable(Iterable.class, LuaConversions::toJavaIterable);
    coerceTable(Set.class, LuaConversions::toJavaSet);
    coerceTable(Map.class, LuaConversions::toJavaMap);
    CoerceLuaToJava.COERCIONS.put(Path.class, new PathCoercion());
  }

  private static <T> void coerceTable(Class<T> clazz, Function<LuaValue, Object> coerce) {
    coerce(clazz, value -> value.type() == LuaValue.TTABLE ? 0 : SCORE_WRONG_TYPE, coerce);
  }

  private static <T> void coerce(Class<T> clazz, ToIntFunction<LuaValue> score,
    Function<LuaValue, Object> coerce) {
    CoerceLuaToJava.COERCIONS.put(clazz, new ContainerCoercion(score, coerce));
  }

  record ContainerCoercion(ToIntFunction<LuaValue> score, Function<LuaValue, Object> coerce)
    implements CoerceLuaToJava.Coercion {

    @Override
    public int score(LuaValue value) {
      return score.applyAsInt(value);
    }

    @Override
    public Object coerce(LuaValue value) {
      return coerce.apply(value);
    }
  }

  private static class PathCoercion implements CoerceLuaToJava.Coercion {
    @Override
    public int score(LuaValue value) {
      return value.isstring() || LuaTables.isArray(value) ? 0 : SCORE_WRONG_TYPE;
    }

    @Override
    public Object coerce(LuaValue value) {
      if (value.isstring()) {
        return Path.of(value.tojstring());
      }
      int len = value.length();
      String main = value.get(1).tojstring();
      String[] next = new String[len - 1];
      for (int i = 1; i < len; i++) {
        next[i - 1] = value.get(i + 1).tojstring();
      }
      return Path.of(main, next);
    }
  }
}
