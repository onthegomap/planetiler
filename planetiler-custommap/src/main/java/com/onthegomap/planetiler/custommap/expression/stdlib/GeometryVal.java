package com.onthegomap.planetiler.custommap.expression.stdlib;

import static org.projectnessie.cel.common.types.Err.newTypeConversionError;
import static org.projectnessie.cel.common.types.Err.noSuchOverload;
import static org.projectnessie.cel.common.types.Types.boolOf;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.WithGeometry;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import com.onthegomap.planetiler.util.ToDoubleFunctionThatThrows;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Geometry;
import org.projectnessie.cel.checker.Decls;
import org.projectnessie.cel.common.types.DoubleT;
import org.projectnessie.cel.common.types.Err;
import org.projectnessie.cel.common.types.StringT;
import org.projectnessie.cel.common.types.TypeT;
import org.projectnessie.cel.common.types.ref.BaseVal;
import org.projectnessie.cel.common.types.ref.Type;
import org.projectnessie.cel.common.types.ref.Val;
import org.projectnessie.cel.common.types.traits.FieldTester;
import org.projectnessie.cel.common.types.traits.Indexer;

/** Wrapper for a geometry that exposes utility functions to CEL expressions. */
public class GeometryVal extends BaseVal implements Indexer, FieldTester {
  public static final String NAME = "geometry";
  public static final com.google.api.expr.v1alpha1.Type PROTO_TYPE = Decls.newObjectType(NAME);
  private static final Type TYPE = TypeT.newObjectTypeValue(NAME);
  private final WithGeometry geometry;
  private static final Map<String, Field> FIELDS = Stream.of(
    doubleField("lat", geom -> GeoUtils.getWorldLat(geom.worldGeometry().getCoordinate().getY())),
    doubleField("lon", geom -> GeoUtils.getWorldLon(geom.worldGeometry().getCoordinate().getX())),
    doubleField("min_lat", geom -> geom.latLonGeometry().getEnvelopeInternal().getMinY()),
    doubleField("max_lat", geom -> geom.latLonGeometry().getEnvelopeInternal().getMaxY()),
    doubleField("min_lon", geom -> geom.latLonGeometry().getEnvelopeInternal().getMinX()),
    doubleField("max_lon", geom -> geom.latLonGeometry().getEnvelopeInternal().getMaxX()),
    geometryField("bbox", geom -> geom.worldGeometry().getEnvelope()),
    geometryField("centroid", WithGeometry::centroid),
    geometryField("centroid_if_convex", WithGeometry::centroidIfConvex),
    geometryField("validated_polygon", WithGeometry::validatedPolygon),
    geometryField("point_on_surface", WithGeometry::pointOnSurface),
    geometryField("line_midpoint", WithGeometry::lineMidpoint),
    geometryField("innermost_point", geom -> geom.innermostPoint(0.1))
  ).collect(Collectors.toMap(field -> field.name, Function.identity()));

  public static GeometryVal fromWorldGeom(Geometry geometry) {
    return new GeometryVal(WithGeometry.fromWorldGeometry(geometry));
  }

  record Field(String name, com.google.api.expr.v1alpha1.Type type, FunctionThatThrows<WithGeometry, Val> getter) {}

  private static Field doubleField(String name, ToDoubleFunctionThatThrows<WithGeometry> getter) {
    return new Field(name, Decls.Double, geom -> DoubleT.doubleOf(getter.applyAsDouble(geom)));
  }

  private static Field geometryField(String name, FunctionThatThrows<WithGeometry, Geometry> getter) {
    return new Field(name, PROTO_TYPE, geom -> new GeometryVal(WithGeometry.fromWorldGeometry(getter.apply(geom))));
  }

  public GeometryVal(WithGeometry geometry) {
    this.geometry = geometry;
  }

  public static com.google.api.expr.v1alpha1.Type fieldType(String fieldName) {
    var field = FIELDS.get(fieldName);
    return field == null ? null : field.type;
  }

  @Override
  public <T> T convertToNative(Class<T> typeDesc) {
    return typeDesc.isInstance(geometry) ? typeDesc.cast(geometry) : null;
  }

  @Override
  public Val convertToType(Type typeValue) {
    return newTypeConversionError(TYPE, typeValue);
  }

  @Override
  public Val equal(Val other) {
    return boolOf(other instanceof GeometryVal val && Objects.equals(val.geometry, geometry));
  }

  @Override
  public Type type() {
    return TYPE;
  }

  @Override
  public Object value() {
    return geometry;
  }

  @Override
  public Val isSet(Val field) {
    if (!(field instanceof StringT)) {
      return noSuchOverload(this, "isSet", field);
    }
    String fieldName = (String) field.value();
    return boolOf(FIELDS.containsKey(fieldName));
  }

  @Override
  public Val get(Val index) {
    if (!(index instanceof StringT)) {
      return noSuchOverload(this, "get", index);
    }
    String fieldName = (String) index.value();
    try {
      var field = FIELDS.get(fieldName);
      return field.getter.apply(geometry);
    } catch (Exception err) {
      return Err.newErr(err, "Error getting %s", fieldName);
    }
  }

  @Override
  public final boolean equals(Object o) {
    return this == o || (o instanceof GeometryVal val && val.geometry.equals(geometry));
  }

  @Override
  public int hashCode() {
    return geometry.hashCode();
  }
}
