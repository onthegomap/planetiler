/*******************************************************************************
 * Copyright (c) 2011 Luaj.org. All rights reserved.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 ******************************************************************************/
package org.luaj.vm2.lib.jse;

import com.google.common.base.CaseFormat;
import com.google.common.base.Converter;
import com.onthegomap.planetiler.experimental.lua.LuaConversions;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaValue;

/**
 * Modified version of {@link org.luaj.vm2.lib.jse.JavaClass} that uses {@link ConcurrentHashMap} instead of a
 * synchronized map to cache classes to improve performance, and also adds some utilities to improve java interop.
 * <p>
 * LuaValue that represents a Java class.
 * <p>
 * Will respond to get() and set() by returning field values, or java methods.
 * <p>
 * This class is not used directly. It is returned by calls to {@link CoerceJavaToLua#coerce(Object)} when a Class is
 * supplied.
 *
 * @see CoerceJavaToLua
 * @see CoerceLuaToJava
 */
class JavaClass extends JavaInstance {
  private static final Converter<String, String> CAMEL_TO_SNAKE_CASE =
    CaseFormat.LOWER_CAMEL.converterTo(CaseFormat.LOWER_UNDERSCORE);

  static final Map<Class<?>, JavaClass> classes = new ConcurrentHashMap<>();

  static final LuaValue NEW = valueOf("new");

  private final Map<LuaValue, Field> fields = new HashMap<>();
  private final Map<LuaValue, LuaValue> methods = new HashMap<>();
  private final Map<LuaValue, Getter> getters = new HashMap<>();
  private final Map<LuaValue, Setter> setters = new HashMap<>();
  private final Map<LuaValue, Class<?>> innerclasses = new HashMap<>();
  public final boolean bindMethods;

  static JavaClass forClass(Class<?> c) {
    // planetiler change: use ConcurrentHashMap instead of synchronized map to improve performance
    JavaClass j = classes.get(c);
    if (j == null) {
      j = classes.computeIfAbsent(c, JavaClass::new);
    }
    return j;
  }

  JavaClass(Class<?> c) {
    super(c);
    this.jclass = this;
    this.bindMethods = c.isAnnotationPresent(LuaBindMethods.class);
    // planetiler change: compute these maps eagerly
    computeFields();
    computeMethods();
    computeInnerClasses();
  }

  private Map<LuaValue, Field> computeFields() {
    Field[] f = ((Class<?>) m_instance).getFields();
    for (Field fi : f) {
      if (Modifier.isPublic(fi.getModifiers())) {
        fields.put(LuaValue.valueOf(fi.getName()), fi);
        try {
          if (!fi.isAccessible()) {
            fi.setAccessible(true);
          }
        } catch (SecurityException | InaccessibleObjectException s) {
        }
      }
    }

    // planetiler change: add snake_case aliases for camelCase methods
    putAliases(fields);
    return fields;
  }

  private void computeMethods() {
    Map<String, List<JavaMethod>> namedlists = new HashMap<>();
    Class<?> clazz = (Class<?>) m_instance;
    Set<String> recordComponents =
      clazz.isRecord() ? Arrays.stream(clazz.getRecordComponents()).map(c -> c.getName()).collect(Collectors.toSet()) :
        Set.of();
    for (Method mi : clazz.getMethods()) {
      if (Modifier.isPublic(mi.getModifiers())) {
        String name = mi.getName();
        // planetiler change: allow methods annotated with @LuaGetter or @LuaSetter to simulate property access
        // also allow record components to be accessed as properties
        if ((recordComponents.contains(name) || mi.isAnnotationPresent(LuaGetter.class)) &&
          mi.getParameterCount() == 0) {
          getters.put(LuaString.valueOf(name), new Getter(mi));
        } else if (mi.isAnnotationPresent(LuaSetter.class)) {
          setters.put(LuaString.valueOf(name), new Setter(mi, mi.getParameterTypes()[0]));
        }
        namedlists.computeIfAbsent(name, k -> new ArrayList<>()).add(JavaMethod.forMethod(mi));
      }
    }
    Constructor<?>[] c = ((Class<?>) m_instance).getConstructors();
    List<JavaConstructor> list = new ArrayList<>();
    for (Constructor<?> constructor : c) {
      if (Modifier.isPublic(constructor.getModifiers())) {
        list.add(JavaConstructor.forConstructor(constructor));
      }
    }
    switch (list.size()) {
      case 0:
        break;
      case 1:
        methods.put(NEW, list.get(0));
        break;
      default:
        methods.put(NEW,
          JavaConstructor.forConstructors(list.toArray(JavaConstructor[]::new)));
        break;
    }

    for (Entry<String, List<JavaMethod>> e : namedlists.entrySet()) {
      String name = e.getKey();
      List<JavaMethod> classMethods = e.getValue();
      LuaValue luaMethod = classMethods.size() == 1 ?
        classMethods.get(0) :
        JavaMethod.forMethods(classMethods.toArray(JavaMethod[]::new));
      methods.put(LuaValue.valueOf(name), luaMethod);
    }

    // planetiler change: add snake_case aliases for camelCase methods
    putAliases(methods);
    putAliases(getters);
    putAliases(setters);
  }

  private void computeInnerClasses() {
    Class<?>[] c = ((Class<?>) m_instance).getClasses();
    for (Class<?> ci : c) {
      String name = ci.getName();
      String stub = name.substring(Math.max(name.lastIndexOf('$'), name.lastIndexOf('.')) + 1);
      innerclasses.put(LuaValue.valueOf(stub), ci);
    }
  }

  private <T> void putAliases(Map<LuaValue, T> map) {
    for (var entry : List.copyOf(map.entrySet())) {
      String key = entry.getKey().tojstring();
      String key2;
      if (LuaConversions.LUA_AND_NOT_JAVA_KEYWORDS.contains(key)) {
        key2 = key.toUpperCase();
      } else {
        key2 = CAMEL_TO_SNAKE_CASE.convert(key);
      }
      map.putIfAbsent(LuaValue.valueOf(key2), entry.getValue());
    }
  }

  Field getField(LuaValue key) {
    return fields.get(key);
  }

  LuaValue getMethod(LuaValue key) {
    return methods.get(key);
  }

  Map<LuaValue, LuaValue> getMethods() {
    return methods;
  }

  Class<?> getInnerClass(LuaValue key) {
    return innerclasses.get(key);
  }

  public LuaValue getConstructor() {
    return getMethod(NEW);
  }

  public Getter getGetter(LuaValue key) {
    return getters.get(key);
  }

  public Setter getSetter(LuaValue key) {
    return setters.get(key);
  }

  public record Getter(Method method) {

    Object get(Object obj) throws InvocationTargetException, IllegalAccessException {
      return method.invoke(obj);
    }
  }

  public record Setter(Method method, Class<?> type) {

    void set(Object obj, Object value) throws InvocationTargetException, IllegalAccessException {
      method.invoke(obj, value);
    }
  }
}
