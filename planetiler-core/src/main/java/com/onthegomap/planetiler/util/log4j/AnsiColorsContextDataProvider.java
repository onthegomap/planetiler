package com.onthegomap.planetiler.util.log4j;

import com.onthegomap.planetiler.util.AnsiColors;
import java.util.Map;
import org.apache.logging.log4j.core.util.ContextDataProvider;

/**
 * Adds the ANSI color codes from {@link AnsiColors#logPatternColors()} to the context data of every log event, so log
 * patterns color lines only while colors are on ({@code --color}, {@code NO_COLOR}).
 */
public class AnsiColorsContextDataProvider implements ContextDataProvider {

  @Override
  public Map<String, String> supplyContextData() {
    return AnsiColors.logPatternColors();
  }
}
