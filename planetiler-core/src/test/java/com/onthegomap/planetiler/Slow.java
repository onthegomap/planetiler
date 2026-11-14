package com.onthegomap.planetiler;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import org.junit.jupiter.api.Tag;

/**
 * Add to any junit test classes or methods to exclude when run with {@code scripts/fasttests.sh --skip-slow} or with
 * extra {@code -Dgroups=!slow} argument.
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Tag("slow")
@Retention(RetentionPolicy.RUNTIME)
public @interface Slow {
}
