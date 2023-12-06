package com.onthegomap.planetiler.experimental.lua;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaNumber;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.lib.jse.LuaBindMethods;
import org.luaj.vm2.lib.jse.LuaFunctionType;
import org.luaj.vm2.lib.jse.LuaGetter;
import org.luaj.vm2.lib.jse.LuaSetter;

@SuppressWarnings("unused")
class GenerateLuaTypesTest {

  @Test
  void testSimpleClass() {
    interface Test {

      String string();

      int intMethod(Integer n);

      long longMethod(Long n);

      double doubleMethod(Double n);

      float floatMethod(Float n);

      short shortMethod(Short n);

      byte byteMethod(Byte n);

      boolean booleanMethod(Boolean n);
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test = {}
      ---@param n boolean
      ---@return boolean
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:boolean_method(n) end
      ---@param n integer
      ---@return integer
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:byte_method(n) end
      ---@param n number
      ---@return number
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:double_method(n) end
      ---@param n number
      ---@return number
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:float_method(n) end
      ---@param n integer
      ---@return integer
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:int_method(n) end
      ---@param n integer
      ---@return integer
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:long_method(n) end
      ---@param n integer
      ---@return integer
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:short_method(n) end
      ---@return string
      function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Test:string() end
      """, Test.class);
  }

  @Test
  void testSimpleClassWithLuaBindMethods() {
    @LuaBindMethods
    interface TestBindMethods {

      String string();

      int intMethod(Integer n);

      void voidMethod();
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestBindMethods
      ---@field int_method fun(n: integer): integer
      ---@field string fun(): string
      ---@field void_method fun(): nil
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestBindMethods = {}
      """, TestBindMethods.class);
  }

  @Test
  void testSimpleClassWithField() {
    class WithField {

      public Date field;
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithField
      ---@field field java_util_Date
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithField = {}
      """, WithField.class);
  }

  @Test
  void testSimpleClassWithGetter() {
    class WithGetter {

      @LuaGetter
      public Date field() {
        return null;
      }

      @LuaGetter
      public List<String> field2() {
        return null;
      }

      @LuaSetter
      public void field2(List<String> value) {}
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithGetter
      ---@field field java_util_Date
      ---@field field2 string[]
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithGetter = {}
      """, WithGetter.class);
  }

  @Test
  void testSimpleClassWithSetter() {
    class WithSetter {

      @LuaSetter
      public void field(Date field) {}
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithSetter
      ---@field field java_util_Date
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithSetter = {}
      """, WithSetter.class);
  }

  @Test
  void testArrays() {
    class WithArrays {

      public int[] intArray;
      public Integer[] intObjectArray;
      public Semaphore[] semaphoreArray;
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithArrays
      ---@field int_array integer[]
      ---@field int_object_array integer[]
      ---@field semaphore_array java_util_concurrent_Semaphore[]
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithArrays = {}
      """, WithArrays.class);
  }

  @Test
  void testKeywordCollision() {
    class KeywordCollision {

      public int and;
    }
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1KeywordCollision
        ---@field AND integer
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1KeywordCollision = {}
        """,
      KeywordCollision.class);
  }

  @Test
  void testLists() {
    class WithLists {

      public List<Integer> ints;
      public List<Date> dates;
      public List<Object> objs;
      public List<?> wildcards;
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithLists
      ---@field dates java_util_Date[]
      ---@field ints integer[]
      ---@field objs any[]
      ---@field wildcards any[]
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1WithLists = {}
      """, WithLists.class);
  }

  @Test
  void testMaps() {
    class WithLists {

      public Map<String, Integer> stringToInt;
      public Map<Date, Double> dateToDouble;
      public Map<Object, ?> objectToWildcard;
      public Map<String, List<List<String>>> stringToDoubleStringList;
    }
    assertGenerated("""
      ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_2WithLists
      ---@field date_to_double {[java_util_Date]: number}
      ---@field object_to_wildcard {[any]: any}
      ---@field string_to_double_string_list {[string]: string[][]}
      ---@field string_to_int {[string]: integer}
      types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_2WithLists = {}
      """, WithLists.class);
  }

  @Test
  void testMethodOnGenericType() {
    class Generic<T> {

      public T value() {
        return null;
      }
    }
    class Concrete extends Generic<String> {}
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Concrete : com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Generic
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Concrete = {}
        ---@return string
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Concrete:value() end
        """,
      Concrete.class);
  }

  @Test
  void testGenericMethod() {
    class GenericMethod {

      public <T> T apply(T input) {
        return null;
      }

      public <T extends Number> T applyNumber(T input) {
        return null;
      }
    }
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1GenericMethod
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1GenericMethod = {}
        ---@generic T
        ---@param input T
        ---@return T
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1GenericMethod:apply(input) end
        ---@generic T : number
        ---@param input T
        ---@return T
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1GenericMethod:apply_number(input) end
        """,
      GenericMethod.class);
  }

  @Test
  void testGenericMethodInterface() {
    interface Interface<T> {

      T value();
    }
    interface ConcreteInterface extends Interface<String> {}
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1ConcreteInterface : com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Interface
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1ConcreteInterface = {}
        ---@return string
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1ConcreteInterface:value() end
        """,
      ConcreteInterface.class);
  }

  @Test
  void testRecord() {
    record Record(int x, int y) {}
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Record : java_lang_Record
        ---@field x integer
        ---@field y integer
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1Record = {}
        """,
      Record.class);
  }

  @Test
  void testGenericField() {
    class Generic<T> {

      public T field;
    }
    class ConcreteField extends Generic<Integer> {}
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1ConcreteField : com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_2Generic
        ---@field field integer
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1ConcreteField = {}
        """,
      ConcreteField.class);
  }

  @Test
  void testLuaValueWithAnnotation() {
    class Target {
      public int method(int arg) {
        return 1;
      }
    }
    class LuaValues {

      public LuaString string;
      public LuaInteger integer;
      public LuaDouble number;
      public LuaNumber number2;
      public LuaTable table;
      @LuaFunctionType(
        target = Target.class,
        method = "method"
      )
      public LuaValue value;
    }
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1LuaValues
        ---@field integer integer
        ---@field number number
        ---@field number2 number
        ---@field string string
        ---@field table table
        ---@field value fun(arg: integer): integer
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1LuaValues = {}
        """,
      LuaValues.class);
  }

  public static class StaticClass {
    public static final int CONSTANT = 1;
    public static int staticField;
    public int instanceField;

    public StaticClass() {}

    public StaticClass(int i) {}

    public int instance(int arg) {
      return 1;
    }

    public static int staticMethod(int arg) {
      return 1;
    }
  }

  @Test
  void testStaticLuaInstanceWithConsructors() {
    assertGeneratedStatic(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass__class
        ---@field CONSTANT integer
        ---@field static_field integer
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass__class = {}
        ---@param i integer
        ---@return com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass__class:new(i) end
        ---@return com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass__class:new() end
        ---@param arg integer
        ---@return integer
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_StaticClass__class:static_method(arg) end
                """
        .trim(),
      StaticClass.class);
  }

  @Test
  void testGenericClassMethod() {
    assertGenerated(
      """
        ---@class (exact) java_util_function_Consumer
        types.java_util_function_Consumer = {}
        ---@param arg0 any
        ---@return nil
        function types.java_util_function_Consumer:accept(arg0) end
        ---@param arg0 java_util_function_Consumer
        ---@return java_util_function_Consumer
        function types.java_util_function_Consumer:and_then(arg0) end
        """,
      Consumer.class);
  }

  @Test
  void testEnum() {
    enum TestEnum {
      A,
      B,
      C
    }
    assertGenerated(
      """
        ---@alias com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum
        ---|com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum
        ---|integer
        ---|"A"
        ---|"B"
        ---|"C"
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum : java_lang_Enum
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum = {}
        ---@param arg0 any
        ---@return integer
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum:compare_to(arg0) end
        ---@param arg0 com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum
        ---@return integer
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum:compare_to(arg0) end
        ---@return java_util_Optional
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum:describe_constable() end
        ---@return userdata
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum:get_declaring_class() end
        ---@return string
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum:name() end
        ---@return integer
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum__enum:ordinal() end
        """,
      TestEnum.class);
    class UsesEnum {
      public TestEnum field;

      public TestEnum method(TestEnum arg) {
        return null;
      }
    }
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1UsesEnum
        ---@field field com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1UsesEnum = {}
        ---@param arg com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum
        ---@return com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1TestEnum
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1UsesEnum:method(arg) end
        """,
      UsesEnum.class);
  }

  @Test
  void testPath() {
    class UsesPath {
      public Path field;

      public Path method(Path arg) {
        return null;
      }
    }
    assertGenerated(
      """
        ---@class (exact) com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1UsesPath
        ---@field field java_nio_file_Path
        types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1UsesPath = {}
        ---@param arg java_nio_file_Path|string|string[]
        ---@return java_nio_file_Path
        function types.com_onthegomap_planetiler_experimental_lua_GenerateLuaTypesTest_1UsesPath:method(arg) end
        """,
      UsesPath.class);
  }

  @Test
  void testGeneratedMetaFileCompiles() {
    String types = new GenerateLuaTypes().generatePlanetiler().toString();
    LuaEnvironment.loadScript(Arguments.of("luajc", "false"), types, "types.lua");
  }

  private static void assertGenerated(String expected, Class<?> clazz) {
    assertGenerated(expected, clazz, false);
  }

  private static void assertGeneratedStatic(String expected, Class<?> clazz) {
    assertGenerated(expected, clazz, true);
  }

  private static void assertGenerated(String expected, Class<?> clazz, boolean staticType) {
    var g = new GenerateLuaTypes();
    var actual = (staticType ? g.getStaticTypeDefinition(clazz) : g.getTypeDefinition(clazz)).trim();
    assertEquals(fixNewlines(expected.trim()), fixNewlines(actual), "got:%n%n%s%n%n".formatted(actual));
  }

  private static String fixNewlines(String input) {
    return input.replaceAll("[\n\r]]", System.lineSeparator());
  }
}
