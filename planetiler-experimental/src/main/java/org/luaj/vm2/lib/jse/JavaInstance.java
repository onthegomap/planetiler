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

import static com.onthegomap.planetiler.experimental.lua.LuaConversions.toLuaTable;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaTable;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

/**
 * Modified version of {@link JavaInstance} with some tweaks to improve java interop.
 * <p>
 * LuaValue that represents a Java instance.
 * <p>
 * Will respond to get() and set() by returning field values or methods.
 * <p>
 * This class is not used directly. It is returned by calls to {@link CoerceJavaToLua#coerce(Object)} when a subclass of
 * Object is supplied.
 *
 * @see CoerceJavaToLua
 * @see CoerceLuaToJava
 */
class JavaInstance extends LuaUserdata {

  volatile JavaClass jclass;
  private final Map<LuaValue, LuaValue> boundMethods;

  JavaInstance(Object instance) {
    super(instance);
    // planetiler change: when class annotated with @LuaBindMethods, allow methods to be called with instance.method()
    if (m_instance.getClass().isAnnotationPresent(LuaBindMethods.class)) {
      boundMethods = jclass().getMethods().entrySet().stream().collect(Collectors.toMap(
        Map.Entry::getKey,
        entry -> new BoundMethod(entry.getValue())
      ));
    } else {
      boundMethods = null;
    }
  }

  @Override
  public LuaTable checktable() {
    // planetiler change: allow maps and lists to be accessed as tables
    return switch (m_instance) {
      case Collection<?> c -> toLuaTable(c);
      case Map<?, ?> m -> toLuaTable(m);
      default -> super.checktable();
    };
  }

  // planetiler change: allow methods on classes annotated with @LuaBindMethods to be called with instance.method()
  private class BoundMethod extends LuaFunction {

    private final LuaValue method;

    BoundMethod(LuaValue method) {
      this.method = method;
    }

    @Override
    public LuaValue call() {
      return method.call(JavaInstance.this);
    }

    @Override
    public LuaValue call(LuaValue arg) {
      return method.call(JavaInstance.this, arg);
    }

    @Override
    public LuaValue call(LuaValue arg1, LuaValue arg2) {
      return method.call(JavaInstance.this, arg1, arg2);
    }

    @Override
    public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
      return method.invoke(LuaValue.varargsOf(new LuaValue[]{JavaInstance.this, arg1, arg2, arg3})).arg(1);
    }

    @Override
    public Varargs invoke(Varargs args) {
      return method.invoke(LuaValue.varargsOf(JavaInstance.this, args));
    }
  }

  private JavaClass jclass() {
    if (jclass == null) {
      synchronized (this) {
        if (jclass == null) {
          jclass = JavaClass.forClass(m_instance.getClass());
        }
      }
    }
    return jclass;
  }

  public LuaValue get(LuaValue key) {
    // planetiler change: allow lists to be accessed as tables
    if (key.isnumber() && m_instance instanceof List<?> c) {
      int idx = key.toint();
      return idx <= 0 || idx > c.size() ? LuaValue.NIL : CoerceJavaToLua.coerce(c.get(idx - 1));
    }
    JavaClass clazz = jclass();
    Field f = clazz.getField(key);
    if (f != null)
      try {
        return CoerceJavaToLua.coerce(f.get(m_instance));
      } catch (Exception e) {
        throw new LuaError(e);
      }
    // planetiler change: allow getter methods
    var getter = clazz.getGetter(key);
    if (getter != null) {
      try {
        return CoerceJavaToLua.coerce(getter.get(m_instance));
      } catch (Exception e) {
        throw new LuaError(e);
      }
    }
    LuaValue m = boundMethods != null ? boundMethods.get(key) : clazz.getMethod(key);
    if (m != null)
      return m;
    Class<?> c = clazz.getInnerClass(key);
    if (c != null)
      return JavaClass.forClass(c);

    // planetiler change: allow maps to be accessed as tables
    if (m_instance instanceof Map<?, ?> map) {
      Object key2 = CoerceLuaToJava.coerce(key, Object.class);
      if (key2 != null) {
        Object value = map.get(key2);
        if (value != null) {
          return CoerceJavaToLua.coerce(value);
        }
      }
    }
    return super.get(key);
  }

  public void set(LuaValue key, LuaValue value) {
    // planetiler change: allow lists to be accessed as tables
    if (key.isnumber() && m_instance instanceof List c) {
      c.set(key.toint() - 1, CoerceLuaToJava.coerce(value, Object.class));
      return;
    }
    JavaClass clazz = jclass();
    Field f = clazz.getField(key);
    if (f != null)
      try {
        f.set(m_instance, CoerceLuaToJava.coerce(value, f.getType()));
        return;
      } catch (Exception e) {
        throw new LuaError(e);
      }
    // planetiler change: allow setter methods
    var setter = clazz.getSetter(key);
    if (setter != null) {
      try {
        setter.set(m_instance, CoerceLuaToJava.coerce(value, setter.type()));
        return;
      } catch (Exception e) {
        throw new LuaError(e);
      }
    }

    // planetiler change: allow maps to be accessed as tables
    if (m_instance instanceof Map map) {
      Object key2 = CoerceLuaToJava.coerce(key, Object.class);
      if (key2 != null) {
        Object value2 = CoerceLuaToJava.coerce(value, Object.class);
        if (value2 == null) {
          map.remove(key2);
        } else {
          map.put(key2, value2);
        }
        return;
      }
    }
    super.set(key, value);
  }

}
