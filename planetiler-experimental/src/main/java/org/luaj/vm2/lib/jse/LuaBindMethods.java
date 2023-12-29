package org.luaj.vm2.lib.jse;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation that allows methods on a class to be called from lua with instance.method(), or for those methods to be
 * detached from the instance and called on their own.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface LuaBindMethods {
}
