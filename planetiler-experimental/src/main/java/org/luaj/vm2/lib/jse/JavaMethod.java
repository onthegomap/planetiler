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

import java.lang.reflect.InaccessibleObjectException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.luaj.vm2.LuaError;
import org.luaj.vm2.LuaFunction;
import org.luaj.vm2.LuaValue;
import org.luaj.vm2.Varargs;

/**
 * Modified version of {@link JavaMethod} with concurrency fixes and a fix to how varargs are handled.
 * <p>
 * LuaValue that represents a Java method.
 * <p>
 * Can be invoked via call(LuaValue...) and related methods.
 * <p>
 * This class is not used directly. It is returned by calls to calls to {@link JavaInstance#get(LuaValue key)} when a
 * method is named.
 *
 * @see CoerceJavaToLua
 * @see CoerceLuaToJava
 */
class JavaMethod extends JavaMember {

  @Override
  public String toString() {
    return "JavaMethod(" + method + ")";
  }

  // planetiler change: use concurrent hash map instead of synchronized map
  static final Map<Method, JavaMethod> methods = new ConcurrentHashMap<>();

  static JavaMethod forMethod(Method m) {
    JavaMethod j = methods.get(m);
    if (j == null)
      j = methods.computeIfAbsent(m, JavaMethod::new);
    return j;
  }

  static LuaFunction forMethods(JavaMethod[] m) {
    return new Overload(m);
  }

  final Method method;

  JavaMethod(Method m) {
    super(m.getParameterTypes(), m.getModifiers());
    this.method = m;
    try {
      if (!m.isAccessible())
        m.setAccessible(true);
    } catch (SecurityException | InaccessibleObjectException s) {
    }
  }

  public LuaValue call() {
    return error("method cannot be called without instance");
  }

  public LuaValue call(LuaValue arg) {
    return invokeMethod(arg.checkuserdata(), LuaValue.NONE);
  }

  public LuaValue call(LuaValue arg1, LuaValue arg2) {
    return invokeMethod(arg1.checkuserdata(), arg2);
  }

  public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
    return invokeMethod(arg1.checkuserdata(), LuaValue.varargsOf(arg2, arg3));
  }

  public Varargs invoke(Varargs args) {
    return invokeMethod(args.checkuserdata(1), args.subargs(2));
  }

  @Override
  protected Object[] convertArgs(Varargs args) {
    Object[] a;
    if (varargs == null) {
      a = new Object[fixedargs.length];
      for (int i = 0; i < a.length; i++)
        a[i] = fixedargs[i].coerce(args.arg(i + 1));
    } else {
      // planetiler fix: pass last arg through as vararg array parameter
      a = new Object[fixedargs.length + 1];
      for (int i = 0; i < fixedargs.length; i++)
        a[i] = fixedargs[i].coerce(args.arg(i + 1));
      a[a.length - 1] = varargs.coerce(LuaValue.listOf(null, args.subargs(fixedargs.length + 1)));
    }
    return a;
  }

  @Override
  int score(Varargs args) {
    int n = args.narg();
    int s = n > fixedargs.length && varargs == null ? CoerceLuaToJava.SCORE_WRONG_TYPE * (n - fixedargs.length) : 0;
    for (int j = 0; j < fixedargs.length; j++)
      s += fixedargs[j].score(args.arg(j + 1));
    // planetiler fix: use component coercion, not array coercion
    if (varargs instanceof CoerceLuaToJava.ArrayCoercion arrayCoercion)
      for (int k = fixedargs.length; k < n; k++)
        s += arrayCoercion.componentCoercion.score(args.arg(k + 1));
    return s;
  }

  LuaValue invokeMethod(Object instance, Varargs args) {
    Object[] a = convertArgs(args);
    try {
      return CoerceJavaToLua.coerce(method.invoke(instance, a));
    } catch (InvocationTargetException e) {
      throw new LuaError(e.getTargetException());
    } catch (Exception e) {
      return LuaValue.error("coercion error " + e);
    }
  }

  /**
   * LuaValue that represents an overloaded Java method.
   * <p>
   * On invocation, will pick the best method from the list, and invoke it.
   * <p>
   * This class is not used directly. It is returned by calls to calls to {@link JavaInstance#get(LuaValue key)} when an
   * overloaded method is named.
   */
  static class Overload extends LuaFunction {

    final JavaMethod[] methods;

    Overload(JavaMethod[] methods) {
      this.methods = methods;
    }

    public LuaValue call() {
      return error("method cannot be called without instance");
    }

    public LuaValue call(LuaValue arg) {
      return invokeBestMethod(arg.checkuserdata(), LuaValue.NONE);
    }

    public LuaValue call(LuaValue arg1, LuaValue arg2) {
      return invokeBestMethod(arg1.checkuserdata(), arg2);
    }

    public LuaValue call(LuaValue arg1, LuaValue arg2, LuaValue arg3) {
      return invokeBestMethod(arg1.checkuserdata(), LuaValue.varargsOf(arg2, arg3));
    }

    public Varargs invoke(Varargs args) {
      return invokeBestMethod(args.checkuserdata(1), args.subargs(2));
    }

    private LuaValue invokeBestMethod(Object instance, Varargs args) {
      JavaMethod best = null;
      int score = Integer.MAX_VALUE;
      for (int i = 0; i < methods.length; i++) {
        int s = methods[i].score(args);
        if (s < score) {
          score = s;
          best = methods[i];
          if (score == 0)
            break;
        }
      }

      // any match?
      if (best == null)
        LuaValue.error("no coercible public method");

      // invoke it
      return best.invokeMethod(instance, args);
    }
  }

}
