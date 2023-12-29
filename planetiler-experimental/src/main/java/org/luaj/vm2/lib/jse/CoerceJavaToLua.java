/*******************************************************************************
 * Copyright (c) 2009-2011 Luaj.org. All rights reserved.
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

import static java.util.Map.entry;

import java.util.Map;
import org.luaj.vm2.LuaDouble;
import org.luaj.vm2.LuaInteger;
import org.luaj.vm2.LuaString;
import org.luaj.vm2.LuaUserdata;
import org.luaj.vm2.LuaValue;

/**
 * Modified version of {@link CoerceJavaToLua} that fixes a thread safety issue around concurrent map updates.
 * <p>
 * Helper class to coerce values from Java to lua within the luajava library.
 * <p>
 * This class is primarily used by the {@link org.luaj.vm2.lib.jse.LuajavaLib}, but can also be used directly when
 * working with Java/lua bindings.
 * <p>
 * To coerce scalar types, the various, generally the {@code valueOf(type)} methods on {@link LuaValue} may be used:
 * <ul>
 * <li>{@link LuaValue#valueOf(boolean)}</li>
 * <li>{@link LuaValue#valueOf(byte[])}</li>
 * <li>{@link LuaValue#valueOf(double)}</li>
 * <li>{@link LuaValue#valueOf(int)}</li>
 * <li>{@link LuaValue#valueOf(String)}</li>
 * </ul>
 * <p>
 * To coerce arrays of objects and lists, the {@code listOf(..)} and {@code tableOf(...)} methods on {@link LuaValue}
 * may be used:
 * <ul>
 * <li>{@link LuaValue#listOf(LuaValue[])}</li>
 * <li>{@link LuaValue#listOf(LuaValue[], org.luaj.vm2.Varargs)}</li>
 * <li>{@link LuaValue#tableOf(LuaValue[])}</li>
 * <li>{@link LuaValue#tableOf(LuaValue[], LuaValue[], org.luaj.vm2.Varargs)}</li>
 * </ul>
 * The method {@link CoerceJavaToLua#coerce(Object)} looks as the type and dimesioning of the argument and tries to
 * guess the best fit for corrsponding lua scalar, table, or table of tables.
 *
 * @see CoerceJavaToLua#coerce(Object)
 * @see org.luaj.vm2.lib.jse.LuajavaLib
 */
public class CoerceJavaToLua {

  interface Coercion {
    LuaValue coerce(Object javaValue);
  };

  private static final class BoolCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      Boolean b = (Boolean) javaValue;
      return b ? LuaValue.TRUE : LuaValue.FALSE;
    }
  }

  private static final class IntCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      Number n = (Number) javaValue;
      return LuaInteger.valueOf(n.intValue());
    }
  }

  private static final class CharCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      Character c = (Character) javaValue;
      return LuaInteger.valueOf(c.charValue());
    }
  }

  private static final class DoubleCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      Number n = (Number) javaValue;
      return LuaDouble.valueOf(n.doubleValue());
    }
  }

  private static final class StringCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      return LuaString.valueOf(javaValue.toString());
    }
  }

  private static final class BytesCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      return LuaValue.valueOf((byte[]) javaValue);
    }
  }

  private static final class ClassCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      return JavaClass.forClass((Class<?>) javaValue);
    }
  }

  private static final class InstanceCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      return new JavaInstance(javaValue);
    }
  }

  private static final class ArrayCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      // should be userdata?
      return new JavaArray(javaValue);
    }
  }

  private static final class LuaCoercion implements Coercion {
    public LuaValue coerce(Object javaValue) {
      return (LuaValue) javaValue;
    }
  }


  // planetiler change: use immutable thread-safe map
  private static final Map<Class<?>, Coercion> COERCIONS;

  static {
    Coercion boolCoercion = new BoolCoercion();
    Coercion intCoercion = new IntCoercion();
    Coercion charCoercion = new CharCoercion();
    Coercion doubleCoercion = new DoubleCoercion();
    Coercion stringCoercion = new StringCoercion();
    Coercion bytesCoercion = new BytesCoercion();
    Coercion classCoercion = new ClassCoercion();
    COERCIONS = Map.ofEntries(
      entry(Boolean.class, boolCoercion),
      entry(Byte.class, intCoercion),
      entry(Character.class, charCoercion),
      entry(Short.class, intCoercion),
      entry(Integer.class, intCoercion),
      entry(Long.class, doubleCoercion),
      entry(Float.class, doubleCoercion),
      entry(Double.class, doubleCoercion),
      entry(String.class, stringCoercion),
      entry(byte[].class, bytesCoercion),
      entry(Class.class, classCoercion)
    );
  }

  /**
   * Coerse a Java object to a corresponding lua value.
   * <p>
   * Integral types {@code boolean}, {@code byte}, {@code char}, and {@code int} will become {@link LuaInteger};
   * {@code long}, {@code float}, and {@code double} will become {@link LuaDouble}; {@code String} and {@code byte[]}
   * will become {@link LuaString}; types inheriting from {@link LuaValue} will be returned without coercion; other
   * types will become {@link LuaUserdata}.
   *
   * @param o Java object needing conversion
   * @return {@link LuaValue} corresponding to the supplied Java value.
   * @see LuaValue
   * @see LuaInteger
   * @see LuaDouble
   * @see LuaString
   * @see LuaUserdata
   */
  public static LuaValue coerce(Object o) {
    if (o == null)
      return LuaValue.NIL;
    Class<?> clazz = o.getClass();
    // planetiler change: don't modify coercions
    Coercion c = COERCIONS.get(clazz);
    if (c == null) {
      c = o instanceof LuaValue ? luaCoercion : clazz.isArray() ? arrayCoercion : instanceCoercion;
    }
    return c.coerce(o);
  }

  static final Coercion instanceCoercion = new InstanceCoercion();

  static final Coercion arrayCoercion = new ArrayCoercion();

  static final Coercion luaCoercion = new LuaCoercion();
}
