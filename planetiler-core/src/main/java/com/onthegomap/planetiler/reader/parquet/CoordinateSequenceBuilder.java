package com.onthegomap.planetiler.reader.parquet;

import com.carrotsearch.hppc.DoubleArrayList;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.CoordinateXYZM;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;

/** A mutable {@link CoordinateSequence} that grows to fit new elements when {@code set*} methods are called. */
class CoordinateSequenceBuilder extends PackedCoordinateSequence {

  private final DoubleArrayList points = new DoubleArrayList();
  private final int components;

  public CoordinateSequenceBuilder(int components) {
    super(components, components > 3 ? 1 : 0);
    this.components = components;
  }


  @Override
  public double getOrdinate(int index, int ordinateIndex) {
    return points.get(index * components + ordinateIndex);
  }

  @Override
  public int size() {
    return points.size() / components;
  }

  @Override
  protected Coordinate getCoordinateInternal(int index) {
    return switch (dimension) {
      case 2 -> new CoordinateXY(getX(index), getY(index));
      case 3 -> new Coordinate(getX(index), getY(index), getZ(index));
      case 4 -> new CoordinateXYZM(getX(index), getY(index), getZ(index), getM(index));
      default -> throw new IllegalStateException("Unexpected value: " + dimension);
    };
  }

  @Override
  public Object clone() {
    throw new UnsupportedOperationException();
  }

  @Override
  public PackedCoordinateSequence copy() {
    return new PackedCoordinateSequence.Double(points.toArray(), components, components > 3 ? 1 : 0);
  }

  @Override
  public void setOrdinate(int index, int ordinate, double value) {
    int idx = index * components + ordinate;
    int cnt = (index + 1) * components;
    points.elementsCount = Math.max(points.elementsCount, cnt);
    points.ensureCapacity(cnt);
    points.set(idx, value);
  }

  @Override
  public Envelope expandEnvelope(Envelope env) {
    for (int i = 0; i < points.size(); i += dimension) {
      env.expandToInclude(points.get(i), points.get(i + 1));
    }
    return env;
  }
}
