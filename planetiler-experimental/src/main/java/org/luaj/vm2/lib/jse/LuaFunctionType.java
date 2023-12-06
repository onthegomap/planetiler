package org.luaj.vm2.lib.jse;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that that generates the type for a lua value from a method on another class.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.FIELD)
public @interface LuaFunctionType {
  Class<?> target();

  String method() default "";
}
