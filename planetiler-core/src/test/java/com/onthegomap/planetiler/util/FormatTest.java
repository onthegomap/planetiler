package com.onthegomap.planetiler.util;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.Duration;
import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.locationtech.jts.geom.CoordinateXY;

class FormatTest {

  private Locale defaultLocale;

  @ParameterizedTest
  @CsvSource({
    "1.5,1,en",
    "999,999,en",
    "1000,1k,en",
    "9999,9.9k,en",
    "10001,10k,en",
    "99999,99k,en",
    "999999,999k,en",
    "9999999,9.9M,en",
    "-9999999,-,en",
    "5.5e12,5.5T,en",
    "5.5e12,'5,5T',fr",
  })
  void testFormatNumeric(Double number, String expected, Locale locale) {
    assertEquals(expected, Format.forLocale(locale).numeric(number, false));
  }

  @ParameterizedTest
  @CsvSource({
    "999,999,en",
    "1000,1k,en",
    "9999,9.9k,en",
    "5.5e9,5.5G,en",
    "5.5e9,'5,5G',fr",
  })
  void testFormatStorage(Double number, String expected, Locale locale) {
    assertEquals(expected, Format.forLocale(locale).storage(number, false));
  }

  @ParameterizedTest
  @CsvSource({
    "0,0%,en",
    "1,100%,en",
    "0.11111,11%,en",
    "0.11111,11 %,fr",
  })
  void testFormatPercent(Double number, String formatted, Locale locale) {
    assertEquals(formatted, Format.forLocale(locale).percent(number));
  }

  @ParameterizedTest
  @CsvSource({
    "a,0,a",
    "a,1,a",
    "a,2,' a'",
    "a,3,'  a'",
    "ab,3,' ab'",
    "abc,3,'abc'",
  })
  void testPad(String in, Integer size, String out) {
    assertEquals(out, Format.padLeft(in, size));
  }

  @ParameterizedTest
  @CsvSource({
    "0,0,en",
    "0.1,0.1,en",
    "0.11,0.1,en",
    "1111.11,'1,111.1',en",
    "1111.11,'1.111,1',it",
  })
  void testFormatDecimal(Double in, String out, Locale locale) {
    assertEquals(out, Format.forLocale(locale).decimal(in));
  }

  @ParameterizedTest
  @CsvSource({
    "0,0s,en",
    "0.1,0.1s,en",
    "0.1,'0,1s',it",
    "0.999,1s,en",
    "1.1,1s,en",
    "59,59s,en",
    "60,1m,en",
    "61.1,1m1s,en",
    "3599,59m59s,en",
    "3600,1h,en",
    "3601,1h1s,en",
  })
  void testFormatDuration(double seconds, String out, Locale locale) {
    assertEquals(out, Format.forLocale(locale).duration(Duration.ofNanos((long) (seconds * NANOSECONDS_PER_SECOND))));
  }

  @BeforeEach
  void getLocale() {
    this.defaultLocale = Locale.getDefault();
  }

  @AfterEach
  void resetLocale() {
    Locale.setDefault(defaultLocale);
  }

  @ParameterizedTest
  @ValueSource(strings = {"en-US", "fr-FR", "en-GB"})
  void testFormat(String locale) {
    Locale.setDefault(Locale.forLanguageTag(locale));
    assertEquals("https://www.openstreetmap.org/#map=1/2.3/1.2", Format.osmDebugUrl(1, new CoordinateXY(1.2, 2.3)));
  }
}
