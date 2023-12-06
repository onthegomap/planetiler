package com.onthegomap.planetiler.experimental.lua;

import static com.onthegomap.planetiler.experimental.lua.LuaConversions.toJava;
import static com.onthegomap.planetiler.experimental.lua.LuaConversions.toLua;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.LuaGetter;
import org.luaj.vm2.lib.jse.LuaSetter;

@SuppressWarnings("unused")
class LuaEnvironmentTests {
  @Test
  void testCallMethod() {
    var env = load("""
      function main()
        return 1
      end
      """);
    assertConvertsTo(1, env.main.call());
    assertConvertsTo("1", env.main.call());
    assertConvertsTo(1L, env.main.call());
  }

  @Test
  void testBindProfile() {
    var env = load("""
      planetiler.output.name = "name"
      planetiler.output.attribution = "attribution"
      planetiler.output.description = "description"
      planetiler.output.version = "version"

      function planetiler.process_feature()
        return 1
      end
      function planetiler.finish()
        return 1
      end
      """);
    assertConvertsTo(1, env.planetiler.process_feature.call());
    assertConvertsTo(1, env.planetiler.finish.call());
    assertEquals("name", env.planetiler.output.name);
    assertEquals("attribution", env.planetiler.output.attribution);
    assertEquals("description", env.planetiler.output.description);
    assertEquals("version", env.planetiler.output.version);
  }

  @Test
  void testOutputPath() {
    var env = load("""
      """);
    assertEquals(Path.of("data", "output.mbtiles"), env.planetiler.output.path);
    var env2 = load("""
      planetiler.output.path = "output.pmtiles"
      """);
    assertEquals(Path.of("output.pmtiles"), env2.planetiler.output.path);
  }

  @Test
  void testCallExposedClassMethod() {
    var env = load("""
      function main()
        return GeoUtils:meters_to_pixel_at_equator(14, 150)
      end
      """);
    assertEquals(15.699197, env.main.call().todouble(), 1e-5);
  }

  @Test
  void testCallJavaMethod() {
    var env = load("""
      function main()
        return obj:call(1) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(int arg) {
        return arg + 1;
      }
    }));
    assertConvertsTo(3, env.main.call());
  }

  @Test
  void testCallJavaMethodUsingLowerSnakeCase() {
    var env = load("""
      function main()
        return obj:call_method(1) + 1
      end
      """, Map.of("obj", new Object() {
      public int callMethod(int arg) {
        return arg + 1;
      }
    }));
    assertConvertsTo(3, env.main.call());
  }

  @Test
  void testCallJavaMethodWith4Args() {
    var env = load("""
      function main()
        return obj:call(1, 2, 3, 4) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(int a, int b, int c, int d) {
        return a + b + c + d;
      }
    }));
    assertConvertsTo(11, env.main.call());
  }

  @Test
  void testPassArrayToJava() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3}) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(int[] args) {
        return IntStream.of(args).sum();
      }
    }));
    assertConvertsTo(7, env.main.call());
  }

  @Test
  void testPassBoxedArrayToJava() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3}) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(Integer[] args) {
        return Stream.of(args).mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(7, env.main.call());
  }

  @Test
  void testPassArrayToLua() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3})
      end
      """, Map.of("obj", new Object() {
      public int[] call(int[] args) {
        return args;
      }
    }));
    assertArrayEquals(new int[]{1, 2, 3}, toJava(env.main.call(), int[].class));
  }

  @Test
  void passListToLua() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3}) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(List<Integer> args) {
        return args.stream().mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(7, env.main.call());
  }

  @Test
  void passJavaListToLua() {
    var env = load("""
      function main(arg)
        return obj:call(arg) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(List<Integer> args) {
        return args.stream().mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(7, env.main.call(toLua(List.of(1, 2, 3))));
  }

  @Test
  void passListToJava() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3})
      end
      """, Map.of("obj", new Object() {
      public List<Integer> call(int[] args) {
        return IntStream.of(args).boxed().toList();
      }
    }));
    assertConvertsTo(List.of(1, 2, 3), env.main.call());
  }

  @Test
  void passCollectionToJava() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3})
      end
      """, Map.of("obj", new Object() {
      public int call(Collection<Integer> args) {
        return args.stream().mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(6, env.main.call());
  }

  @Test
  void passSetToJava() {
    var env = load("""
      function main()
        return obj:call({1, 2, 3, 3})
      end
      """, Map.of("obj", new Object() {
      public int call(Set<Integer> args) {
        return args.stream().mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(6, env.main.call());
  }

  @Test
  void passSetFromTableToJava() {
    var env = load("""
      function main()
        return obj:call({[1] = true, [2] = true, [3] = true})
      end
      """, Map.of("obj", new Object() {
      public int call(Set<Integer> args) {
        return args.stream().mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(6, env.main.call());
  }

  @Test
  void passMapToJava() {
    var env = load("""
      function main()
        return obj:call({[1] = "one", [2] = "two", [3] = "three"})
      end
      """, Map.of("obj", new Object() {
      public String call(Map<Integer, String> args) {
        return args.get(1) + " " + args.get(3);
      }
    }));
    assertConvertsTo("one three", env.main.call());
  }

  @Test
  void testCallPrimitiveJavaVarArgsMethod() {
    var env = load("""
      function main()
        return obj:call(1, 2, 3) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(int... args) {
        return IntStream.of(args).sum();
      }
    }));
    assertConvertsTo(7, env.main.call());
  }

  @Test
  void testCallBoxedJavaVarArgsMethod() {
    var env = load("""
      function main()
        return obj:call(1, 2, 3) + 1
      end
      """, Map.of("obj", new Object() {
      public int call(Integer... args) {
        return Stream.of(args).mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(7, env.main.call());
  }

  @Test
  void chooseVarArgMethodOverOthers() {
    var env = load("""
      function main()
        return {
          obj:call(),
          obj:call(1),
          obj:call(1, 2),
          obj:call(1, 2, 3),
          obj:call(1, 2, 3, 4),
          obj:call(1, 2, 3, 4, 5),
          obj:call(1, 2, 3, 4, 5, 6),
          obj:call(1, 2, 3, 4, 5, 6, 7),
          obj:call(1, 2, 3, 4, 5, 6, 7, 8),
          obj:call(1, 2, 3, 4, 5, 6, 7, 8, 9),
          obj:call(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        }
      end
      """, Map.of("obj", new Object() {
      public int call() {
        return 0;
      }

      public int call(int a) {
        return a;
      }

      public int call(int a, int b) {
        return a + b;
      }

      public int call(int a, int b, int c) {
        return a + b + c;
      }

      public int call(int a, int b, int c, int d) {
        return a + b + c + d;
      }

      public int call(int a, int b, int c, int d, int... rest) {
        return a + b + c + d + IntStream.of(rest).sum();
      }
    }));
    assertConvertsTo(List.class, List.of(
      0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55
    ), env.main.call());
  }

  @Test
  void chooseVarArgMethodOverOthersBoxed() {
    var env = load("""
      function main()
        return {
          obj:call(),
          obj:call(1),
          obj:call(1, 2),
          obj:call(1, 2, 3),
          obj:call(1, 2, 3, 4),
          obj:call(1, 2, 3, 4, 5),
          obj:call(1, 2, 3, 4, 5, 6),
          obj:call(1, 2, 3, 4, 5, 6, 7),
          obj:call(1, 2, 3, 4, 5, 6, 7, 8),
          obj:call(1, 2, 3, 4, 5, 6, 7, 8, 9),
          obj:call(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        }
      end
      """, Map.of("obj", new Object() {
      public Integer call() {
        return 0;
      }

      public Integer call(Integer a) {
        return a;
      }

      public Integer call(Integer a, Integer b) {
        return a + b;
      }

      public Integer call(Integer a, Integer b, Integer c) {
        return a + b + c;
      }

      public Integer call(Integer a, Integer b, Integer c, Integer d) {
        return a + b + c + d;
      }

      public Integer call(Integer a, Integer b, Integer c, Integer d, Integer... rest) {
        return a + b + c + d + Stream.of(rest).mapToInt(i -> i).sum();
      }
    }));
    assertConvertsTo(List.class, List.of(
      0, 1, 3, 6, 10, 15, 21, 28, 36, 45, 55
    ), env.main.call());
  }

  @Test
  void testCoercesPathFromString() {
    var env = load("""
      function main()
        return obj:call("test.java")
      end
      """, Map.of("obj", new Object() {
      public String call(Path path) {
        return path.toString();
      }
    }));
    assertConvertsTo("test.java", env.main.call());
  }

  @Test
  void testCoercesPathFromList() {
    var env = load("""
      function main()
        return obj:call({"a", "b", "c.java"})
      end
      """, Map.of("obj", new Object() {
      public Path call(Path path) {
        return path;
      }
    }));
    assertConvertsTo(Path.of("a", "b", "c.java"), env.main.call());
  }

  @Test
  void testGettersAndSetters() {
    var obj = new Object() {
      public int value = 0;
      private int value2 = 0;

      @LuaSetter
      public void value2(int v) {
        this.value2 = v;
      }

      @LuaGetter
      public int value2() {
        return this.value2 + 1;
      }
    };
    var env = load("""
      function main()
        obj.value = 1
        obj.value2 = 2;
        return obj.value2;
      end
      """, Map.of("obj", obj));
    assertConvertsTo(3, env.main.call());
    assertEquals(1, obj.value);
    assertEquals(2, obj.value2);
  }

  @Test
  void testRecord() {
    record Record(int a, String b) {
      public int c() {
        return a + 1;
      }
    }
    var env = load("""
      function main()
        return {obj.a + 1, obj.b .. 1, obj:c()};
      end
      """, Map.of("obj", new Record(1, "2")));
    assertConvertsTo(List.class, List.of(2, "21", 2), env.main.call());
  }

  @Test
  void testSetLanguages() {
    var env = load("""
      planetiler.languages = {"en", "es"}
      """);
    assertEquals(List.of("en", "es"), env.runner.getDefaultLanguages());
  }

  @Test
  void testFetchWikidataTranslations() {
    var env = load("""
      planetiler.fetch_wikidata_translations()
      planetiler.fetch_wikidata_translations("data/sources/translations.json")
      """);
  }

  @Test
  void testAddSource() {
    var env = load("""
      planetiler.add_source('osm', {
        type = 'osm',
        path = 'file.osm.pbf'
      })
      """);
  }

  @Test
  void testReadArg() {
    var env = LuaEnvironment.loadScript(Arguments.of(
      "key", "value"
    ), """
      function main()
        return planetiler.args:get_string("key")
      end
      """, "script.lua", Map.of());
    assertConvertsTo("value", env.main.call());
  }

  @Test
  void testReadConfigRecord() {
    var env = LuaEnvironment.loadScript(Arguments.of(
      "threads", "99"
    ), """
      function main()
        return planetiler.config.threads
      end
      """, "script.lua", Map.of());
    assertConvertsTo(99, env.main.call());
  }

  @Test
  void testTranslations() {
    var env = load("""
      planetiler.languages = {"en", "es"}
      function main()
        return planetiler.translations:get_translations({
          ['name:en'] = "english name",
          ['name:es'] = "spanish name",
          ['name:de'] = "german name",
        })
      end
      """);
    assertConvertsTo(Map.class, Map.of(
      "name:en", "english name",
      "name:es", "spanish name"
    ), env.main.call());
  }

  @Test
  void transliterate() {
    var env = load("""
      function main()
        return planetiler.translations:transliterate("日本")
      end
      """);
    assertConvertsTo("rì běn", env.main.call());
  }

  @Test
  void testStats() {
    var env = load("""
      planetiler.stats:data_error('lua_error')
      """);
    assertEquals(Map.of(
      "lua_error", 1L
    ), env.planetiler.stats.dataErrors());
  }

  @Test
  void testIterateOverList() {
    var env = load("""
      function main()
        local result = 0
        for i, match in ipairs(obj:call()) do
          result = result + match
        end
        return result
      end
      """, Map.of("obj", new Object() {
      public List<Integer> call() {
        return List.of(1, 2, 3);
      }
    }));
    assertConvertsTo(6, env.main.call());
  }

  @Test
  void testIterateOverMap() {
    var env = load("""
      function main()
        local result = 0
        for k, v in pairs(obj:call()) do
          result = result + k + v
        end
        return result
      end
      """, Map.of("obj", new Object() {
      public Map<Integer, Integer> call() {
        return Map.of(1, 2, 3, 4);
      }
    }));
    assertConvertsTo(10, env.main.call());
  }

  @Test
  void testInvokeReservedKeyword() {
    var env = load("""
      function main()
        return obj:AND()
      end
      """, Map.of("obj", new Object() {
      public int and() {
        return 1;
      }
    }));
    assertConvertsTo(1, env.main.call());
  }

  @Test
  void testPickVarArgOverList() {
    var env = load("""
      function main()
        return {obj:call(), obj:call('a'), obj:call({'a'})}
      end
      """, Map.of("obj", new Object() {
      public int call(String... args) {
        return 1;
      }

      public int call(List<String> args) {
        return 2;
      }
    }));
    assertConvertsTo(List.class, List.of(1, 1, 2), env.main.call());
  }

  @Test
  void testPickVarArgOverListAfterFirstArg() {
    var env = load("""
      function main()
        return {obj:call('a'), obj:call('a', 'a'), obj:call('a', {'a'})}
      end
      """, Map.of("obj", new Object() {
      public int call(String arg, String... args) {
        return 1;
      }

      public int call(String arg, List<String> args) {
        return 2;
      }
    }));
    assertConvertsTo(List.class, List.of(1, 1, 2), env.main.call());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testGetWithIndexFromList(int idx) {
    var env = load("""
      function main()
        return obj:call()[%d]
      end
      """.formatted(idx), Map.of("obj", new Object() {
      public List<Integer> call() {
        return List.of(1);
      }
    }));
    assertConvertsTo(Integer.class, idx == 1 ? 1 : 0, env.main.call());
  }

  @Test
  void testSetWithIndexFromList() {
    var env = load("""
      function main()
        local list = obj:call()
        list[1] = 2
        return list[1]
      end
      """, Map.of("obj", new Object() {
      public List<Integer> call() {
        return new ArrayList<>(List.of(1));
      }
    }));
    assertConvertsTo(2, env.main.call());
  }

  @ParameterizedTest
  @ValueSource(strings = {"a", "b", "c"})
  void testGetFromMap(String value) {
    var env = load("""
      function main()
        return obj:call()["%s"]
      end
      """.formatted(value), Map.of("obj", new Object() {
      public Map<String, Integer> call() {
        return Map.of("b", 1);
      }
    }));
    assertConvertsTo(Integer.class, value.equals("b") ? 1 : 0, env.main.call());
  }

  @Test
  void testSetMap() {
    var env = load("""
      function main()
        local list = obj:call()
        list["a"] = "c"
        return list["a"]
      end
      """, Map.of("obj", new Object() {
      public Map<String, String> call() {
        return new HashMap<>(Map.of("a", "a"));
      }
    }));
    assertConvertsTo("c", env.main.call());
  }

  @ParameterizedTest
  @ValueSource(ints = {0, 1, 2})
  void testGetFromArray(int idx) {
    var env = load("""
      function main()
        return obj:call()[%d]
      end
      """.formatted(idx), Map.of("obj", new Object() {
      public int[] call() {
        return new int[]{1};
      }
    }));
    assertConvertsTo(Integer.class, idx == 1 ? 1 : 0, env.main.call());
  }

  @Test
  void testSetArray() {
    var env = load("""
      function main()
        local list = obj:call()
        list[1] = 2
        return list[1]
      end
      """, Map.of("obj", new Object() {
      public int[] call() {
        return new int[]{1};
      }
    }));
    assertConvertsTo(2, env.main.call());
  }

  @Test
  void testEmoji() {
    var env = load("""
      function main()
        return '👍'
      end
      """, Map.of("obj", new Object() {
      public int[] call() {
        return new int[]{1};
      }
    }));
    assertConvertsTo("👍", env.main.call());
  }

  @Test
  void testArrayLength() {
    var env = load("""
      function main()
        return #{1}
      end
      """, Map.of("obj", new Object() {
      public int[] call() {
        return new int[]{1};
      }
    }));
    assertConvertsTo(1, env.main.call());
  }

  @Test
  void testJavaArrayLength() {
    var env = load("""
      function main()
        return #obj:call()
      end
      """, Map.of("obj", new Object() {
      public int[] call() {
        return new int[]{1};
      }
    }));
    assertConvertsTo(1, env.main.call());
  }

  @Test
  void testJavaListLength() {
    var env = load("""
      function main()
        return #obj:call()
      end
      """, Map.of("obj", new Object() {
      public List<Integer> call() {
        return List.of(1);
      }
    }));
    assertConvertsTo(1, env.main.call());
  }

  @Test
  void testEnum() {
    enum MyEnum {
      A,
      B,
      C
    };
    var env = load("""
      function main()
        return {
           obj:call(0),
           obj:call('B'),
           obj:call(enum.C),
           obj:call2(0)
        }
      end
      """, Map.of("enum", MyEnum.class, "obj", new Object() {

      public int call(MyEnum e) {
        return e.ordinal();
      }

      public MyEnum call2(int ordinal) {
        return MyEnum.values()[ordinal];
      }
    }));
    assertConvertsTo(List.class, List.of(
      0, 1, 2, MyEnum.A
    ), env.main.call());
  }

  public static class MyObj {
    public int value = 1;
    public static int staticValue = 1;

    public static int get() {
      return 1;
    }
  }

  private static <T> void assertConvertsTo(T java, LuaValue lua) {
    assertConvertsTo(java.getClass(), java, lua);
  }

  private static <T> void assertConvertsTo(Class<?> clazz, T java, LuaValue lua) {
    assertEquals(java, toJava(lua, clazz));
  }

  private static LuaEnvironment load(String script) {
    return load(script, Map.of());
  }

  private static LuaEnvironment load(String script, Map<String, ?> extras) {
    return LuaEnvironment.loadScript(Arguments.of(), script, "script.lua", extras);
  }
}
