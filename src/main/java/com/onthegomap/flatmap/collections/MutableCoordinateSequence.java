package com.onthegomap.flatmap.collections;

import com.carrotsearch.hppc.DoubleArrayList;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

public class MutableCoordinateSequence extends PackedCoordinateSequence {

  private final DoubleArrayList points = new DoubleArrayList();

  public MutableCoordinateSequence() {
    super(2, 0);
  }

  public static MutableCoordinateSequence newScalingSequence(double relX, double relY, double scale) {
    return new ScalingSequence(scale, relX, relY);
  }

  @Override
  public double getOrdinate(int index, int ordinateIndex) {
    return points.get((index * 2) + ordinateIndex);
  }

  @Override
  public int size() {
    return points.size() >> 1;
  }

  @Override
  protected Coordinate getCoordinateInternal(int index) {
    return new CoordinateXY(getX(index), getY(index));
  }

  @Override
  @Deprecated
  public Object clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PackedCoordinateSequence copy() {
    return new PackedCoordinateSequence.Double(points.toArray(), dimension, measures);
  }

  @Override
  public void setOrdinate(int index, int ordinate, double value) {
    points.set((index << 1) + ordinate, value);
  }

  @Override
  public Envelope expandEnvelope(Envelope env) {
    return null;
  }

  public void addPoint(double x, double y) {
    int size = size();
    if (size == 0 || getX(size - 1) != x || getY(size - 1) != y) {
      points.add(x, y);
    }
  }

  public void closeRing() {
    int size = size();
    if (size >= 1) {
      double firstX = getX(0);
      double firstY = getY(0);
      double lastX = getX(size - 1);
      double lastY = getY(size - 1);
      if (firstX != lastX || firstY != lastY) {
        points.add(firstX, firstY);
      }
    }
  }

  private static class ScalingSequence extends MutableCoordinateSequence {

    private final double scale;
    private final double relX;
    private final double relY;

    public ScalingSequence(double scale, double relX, double relY) {
      this.scale = scale;
      this.relX = relX;
      this.relY = relY;
    }

    @Override
    public void addPoint(double x, double y) {
      super.addPoint(scale * (x - relX), scale * (y - relY));
    }
  }
}
