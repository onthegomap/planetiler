package com.onthegomap.planetiler.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.util.AnsiColors;
import com.onthegomap.planetiler.util.Format;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongToDoubleFunction;
import org.junit.jupiter.api.Test;

class RateCellTest {
  private final Format format = Format.defaultInstance();
  private final AtomicLong value = new AtomicLong();

  private RateCell cell(LongToDoubleFunction fractionDone, boolean color) {
    return new RateCell(format, value::get, fractionDone, n -> format.numeric(n, true), color);
  }

  @Test
  void testShowsValuePercentAndRate() {
    value.set(50);
    var cell = cell(done -> done / 100d, true);

    assertEquals("[   50  50%    0/s ]", cell.render());
  }

  @Test
  void testLeavesOutPercentWithoutTotal() {
    value.set(50);

    assertEquals("[   50    0/s ]", cell(null, true).render());
  }

  @Test
  void testShowsDashWhilePercentIsUnknown() {
    value.set(50);

    assertEquals("[   50    -    0/s ]", cell(done -> Double.NaN, true).render());
  }

  @Test
  void testGreenWhileMoving() {
    var cell = cell(done -> done / 100d, true);
    value.set(60);

    String moving = cell.render();
    String stopped = cell.render();

    assertTrue(TestUtils.withoutColors(moving).startsWith("[   60  60% "), moving);
    assertEquals(AnsiColors.green(TestUtils.withoutColors(moving)), moving);
    assertEquals("[   60  60%    0/s ]", stopped);
  }

  @Test
  void testGreenWhenPercentMovesWithoutValue() {
    var fraction = new AtomicLong();
    var cell = cell(done -> fraction.get() / 10d, true);
    cell.render();
    fraction.set(5);

    assertEquals(AnsiColors.green("[    0  50%    0/s ]"), cell.render());
  }

  @Test
  void testGreenWhileBusy() {
    value.set(50);

    assertEquals(AnsiColors.green("[   50    0/s ]"), cell(null, true).render(true));
  }

  @Test
  void testNeverGreenWithoutColor() {
    var cell = cell(null, false);
    value.set(60);

    String rendered = cell.render(true);

    assertEquals(TestUtils.withoutColors(rendered), rendered);
  }

  @Test
  void testCounterThatWentBackToZeroCountsAsMoving() {
    value.set(100);
    var cell = cell(null, true);
    value.set(5);

    String reset = cell.render();

    assertEquals(AnsiColors.green(TestUtils.withoutColors(reset)), reset);
  }

  @Test
  void testRateCountsFromStartValue() {
    value.set(1_000);
    // a count that has been at 1,000 for a second, like a download that started before the row got created
    var cell = new RateCell(format, value::get, null, n -> format.numeric(n, true), false, 1_000,
      System.nanoTime() - 1_000_000_000L);

    assertEquals("[   1k    0/s ]", cell.render());
  }

  @Test
  void testWidth() {
    value.set(12_345);

    assertEquals(TestUtils.withoutColors(cell(null, true).render()).length(), cell(null, true).width());
    assertEquals(TestUtils.withoutColors(cell(done -> 0.5, true).render()).length(), cell(done -> 0.5, true).width());
  }
}
