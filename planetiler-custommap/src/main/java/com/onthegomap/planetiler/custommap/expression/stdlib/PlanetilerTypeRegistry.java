package com.onthegomap.planetiler.custommap.expression.stdlib;

import static org.projectnessie.cel.common.types.Err.newErr;
import static org.projectnessie.cel.common.types.Err.unsupportedRefValConversionErr;

import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.Map;
import org.projectnessie.cel.common.types.pb.Db;
import org.projectnessie.cel.common.types.pb.DefaultTypeAdapter;
import org.projectnessie.cel.common.types.ref.FieldType;
import org.projectnessie.cel.common.types.ref.Type;
import org.projectnessie.cel.common.types.ref.TypeRegistry;
import org.projectnessie.cel.common.types.ref.Val;

/** Registers any types that are available to CEL expressions in planetiler configs. */
public final class PlanetilerTypeRegistry implements TypeRegistry {

  @Override
  public TypeRegistry copy() {
    return new PlanetilerTypeRegistry();
  }

  @Override
  public void register(Object t) {
    // types are defined statically
  }

  @Override
  public void registerType(Type... types) {
    // types are defined statically
  }

  @Override
  public Val nativeToValue(Object value) {
    return switch (value) {
      case Val val -> val;
      case SourceFeature sourceFeature -> new GeometryVal(sourceFeature);
      case null, default -> {
        Val val = DefaultTypeAdapter.nativeToValue(Db.defaultDb, this, value);
        if (val != null) {
          yield val;
        }
        yield unsupportedRefValConversionErr(value);
      }
    };
  }

  @Override
  public Val enumValue(String enumName) {
    return newErr("unknown enum name '%s'", enumName);
  }

  @Override
  public Val findIdent(String identName) {
    return null;
  }

  @Override
  public com.google.api.expr.v1alpha1.Type findType(String typeName) {
    return typeName.equals(GeometryVal.NAME) ? GeometryVal.PROTO_TYPE : null;
  }

  @Override
  public FieldType findFieldType(String messageType, String fieldName) {
    com.google.api.expr.v1alpha1.Type type = switch (messageType) {
      case GeometryVal.NAME -> GeometryVal.fieldType(fieldName);
      case null, default -> null;
    };
    return type == null ? null : new FieldType(type, any -> false, any -> null);
  }

  @Override
  public Val newValue(String typeName, Map<String, Val> fields) {
    return null;
  }
}
