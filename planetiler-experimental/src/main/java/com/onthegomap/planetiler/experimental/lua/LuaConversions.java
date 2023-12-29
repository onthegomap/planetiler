package com.onthegomap.planetiler.experimental.lua;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaTables;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.OneArgFunction;
import org.luaj.vm2.lib.jse.CoerceJavaToLua;
import org.luaj.vm2.lib.jse.CoerceLuaToJava;

/**
 * Helper methods to convert between lua and java types.
 */
public interface LuaConversions {

  static LuaValue toLua(Object sourceFeature) {
    return CoerceJavaToLua.coerce(sourceFeature);
  }

  static LuaTable toLuaTable(Collection<?> list) {
    return LuaValue.listOf(list.stream().map(LuaConversions::toLua).toArray(LuaValue[]::new));
  }

  static LuaTable toLuaTable(Map<?, ?> map) {
    return LuaValue.tableOf(map.entrySet().stream()
      .flatMap(entry -> Stream.of(toLua(entry.getKey()), toLua(entry.getValue())))
      .toArray(LuaValue[]::new));
  }

  @SuppressWarnings("unchecked")
  static <T> T toJava(LuaValue value, Class<T> clazz) {
    return (T) CoerceLuaToJava.coerce(value, clazz);
  }

  static List<Object> toJavaList(LuaValue list) {
    return toJavaList(list, Object.class);
  }

  static <T> List<T> toJavaList(LuaValue list, Class<T> itemClass) {
    if (list instanceof LuaUserdata userdata && userdata.userdata() instanceof List<?> fromLua) {
      @SuppressWarnings("unchecked") List<T> result = (List<T>) fromLua;
      return result;
    } else if (list.istable()) {
      int length = list.length();
      List<T> result = new ArrayList<>();
      for (int i = 0; i < length; i++) {
        result.add(toJava(list.get(i + 1), itemClass));
      }
      return result;
    }
    return List.of();
  }

  static Collection<Object> toJavaCollection(LuaValue list) {
    return toJavaCollection(list, Object.class);
  }

  static <T> Collection<T> toJavaCollection(LuaValue list, Class<T> itemClass) {
    if (list instanceof LuaUserdata userdata && userdata.userdata() instanceof Collection<?> fromLua) {
      @SuppressWarnings("unchecked") Collection<T> result = (Collection<T>) fromLua;
      return result;
    } else {
      return toJavaList(list, itemClass);
    }
  }

  static Iterable<Object> toJavaIterable(LuaValue list) {
    return toJavaIterable(list, Object.class);
  }

  static <T> Iterable<T> toJavaIterable(LuaValue list, Class<T> itemClass) {
    if (list instanceof LuaUserdata userdata && userdata.userdata() instanceof Iterable<?> fromLua) {
      @SuppressWarnings("unchecked") Iterable<T> result = (Iterable<T>) fromLua;
      return result;
    } else {
      return toJavaList(list, itemClass);
    }
  }

  static Set<Object> toJavaSet(LuaValue list) {
    return toJavaSet(list, Object.class);
  }

  static <T> Set<T> toJavaSet(LuaValue list, Class<T> itemClass) {
    if (list instanceof LuaUserdata userdata && userdata.userdata() instanceof Set<?> fromLua) {
      @SuppressWarnings("unchecked") Set<T> result = (Set<T>) fromLua;
      return result;
    } else if (list instanceof LuaTable table) {
      Set<T> result = new LinkedHashSet<>();
      if (LuaTables.isArray(table)) {
        int length = list.length();
        for (int i = 0; i < length; i++) {
          result.add(toJava(list.get(i + 1), itemClass));
        }
      } else {
        for (var key : table.keys()) {
          var value = table.get(key);
          if (value.toboolean()) {
            result.add(toJava(key, itemClass));
          }
        }
      }
      return result;
    }
    return Set.of();
  }

  static Map<Object, Object> toJavaMap(LuaValue list) {
    return toJavaMap(list, Object.class, Object.class);
  }

  static <K, V> Map<K, V> toJavaMap(LuaValue list, Class<K> keyClass, Class<V> valueClass) {
    if (list instanceof LuaUserdata userdata && userdata.userdata() instanceof Map<?, ?> fromLua) {
      @SuppressWarnings("unchecked") Map<K, V> result = (Map<K, V>) fromLua;
      return result;
    } else if (list instanceof LuaTable table) {
      Map<K, V> result = new LinkedHashMap<>();
      for (var key : table.keys()) {
        result.put(toJava(key, keyClass), toJava(table.get(key), valueClass));
      }
      return result;
    }
    return Map.of();
  }

  static <T> LuaValue consumerToLua(Consumer<T> consumer, Class<T> itemClass) {
    return new LuaConsumer<>(consumer, itemClass);
  }

  class LuaConsumer<T> extends OneArgFunction {

    private final Class<T> itemClass;
    private final Consumer<T> consumer;

    public LuaConsumer(Consumer<T> consumer, Class<T> itemClass) {
      this.consumer = consumer;
      this.itemClass = itemClass;
    }

    @Override
    public LuaValue call(LuaValue arg) {
      consumer.accept(toJava(arg, itemClass));
      return NIL;
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        (o instanceof LuaConsumer<?> other &&
          itemClass.equals(other.itemClass) &&
          consumer.equals(other.consumer));
    }

    @Override
    public int hashCode() {
      int result = itemClass.hashCode();
      result = 31 * result + consumer.hashCode();
      return result;
    }
  }

  static <I, O> LuaValue functionToLua(Function<I, O> fn, Class<I> inputClass) {
    return new FunctionWrapper<>(fn, inputClass);
  }

  class FunctionWrapper<I, O> extends OneArgFunction {

    private final Class<I> inputClass;
    private final Function<I, O> fn;

    public FunctionWrapper(Function<I, O> fn, Class<I> inputClass) {
      this.fn = fn;
      this.inputClass = inputClass;
    }

    @Override
    public LuaValue call(LuaValue arg) {
      return toLua(fn.apply(toJava(arg, inputClass)));
    }

    @Override
    public boolean equals(Object o) {
      return this == o ||
        (o instanceof FunctionWrapper<?, ?> other &&
          inputClass.equals(other.inputClass) &&
          fn.equals(other.fn));
    }

    @Override
    public int hashCode() {
      int result = inputClass.hashCode();
      result = 31 * result + fn.hashCode();
      return result;
    }
  }
}
