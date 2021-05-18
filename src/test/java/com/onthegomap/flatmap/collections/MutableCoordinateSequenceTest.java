package com.onthegomap.flatmap.collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.carrotsearch.hppc.DoubleArrayList;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

public class MutableCoordinateSequenceTest {

  private static void assertContents(MutableCoordinateSequence seq, double... expected) {
    double[] actual = new double[seq.size() * 2];
    for (int i = 0; i < seq.size(); i++) {
      actual[i * 2] = seq.getX(i);
      actual[i * 2 + 1] = seq.getY(i);
    }
    assertEquals(DoubleArrayList.from(expected), DoubleArrayList.from(actual), "getX/getY");
    PackedCoordinateSequence copy = seq.copy();
    for (int i = 0; i < seq.size(); i++) {
      actual[i * 2] = copy.getX(i);
      actual[i * 2 + 1] = copy.getY(i);
    }
    assertEquals(DoubleArrayList.from(expected), DoubleArrayList.from(actual), "copied getX/getY");
  }

  @Test
  public void testOuter() {
    assertTrue(new MutableCoordinateSequence(false).isInnerRing());
    assertFalse(new MutableCoordinateSequence(true).isInnerRing());
  }

  @Test
  public void testEmpty() {
    var seq = new MutableCoordinateSequence(false);
    assertEquals(0, seq.copy().size());
  }

  @Test
  public void testSingle() {
    var seq = new MutableCoordinateSequence(false);
    seq.addPoint(1, 2);
    assertContents(seq, 1, 2);
  }

  @Test
  public void testTwoPoints() {
    var seq = new MutableCoordinateSequence(false);
    seq.addPoint(1, 2);
    seq.addPoint(3, 4);
    assertContents(seq, 1, 2, 3, 4);
  }

  @Test
  public void testClose() {
    var seq = new MutableCoordinateSequence(false);
    seq.addPoint(1, 2);
    seq.addPoint(3, 4);
    seq.addPoint(0, 1);
    seq.closeRing();
    assertContents(seq, 1, 2, 3, 4, 0, 1, 1, 2);
  }

  @Test
  public void testScaling() {
    var seq = MutableCoordinateSequence.newScalingSequence(true, 1, 2, 3);
    seq.addPoint(1, 2);
    seq.addPoint(3, 4);
    seq.addPoint(0, 1);
    seq.closeRing();
    assertContents(seq, 0, 0, 6, 6, -3, -3, 0, 0);
  }
}
