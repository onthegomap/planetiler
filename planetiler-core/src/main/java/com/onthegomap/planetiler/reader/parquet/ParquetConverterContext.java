package com.onthegomap.planetiler.reader.parquet;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/** Constructs java objects from parquet records at read-time. */
final class ParquetConverterContext {

  final ParquetConverterContext parent;
  final String fieldOnParent;
  final Type type;
  final boolean repeated;
  private final int fieldCount;
  ParquetRecordConverter.Group current;

  ParquetConverterContext(ParquetConverterContext parent, String fieldOnParent, Type type, boolean repeated) {
    this.parent = parent;
    this.fieldOnParent = fieldOnParent;
    this.type = type;
    this.repeated = repeated;
    this.fieldCount = type.isPrimitive() ? 0 : type.asGroupType().getFieldCount();
  }

  public ParquetConverterContext(ParquetConverterContext newParent, Type type) {
    this(newParent, type.getName(), type, type.isRepetition(Type.Repetition.REPEATED));
  }

  public ParquetConverterContext(MessageType schema) {
    this(null, schema);
  }

  public ParquetConverterContext field(int i) {
    return new ParquetConverterContext(this, type.asGroupType().getType(i));
  }

  /** Returns a new context that flattens-out this level of the hierarchy and writes values into the parent field. */
  public ParquetConverterContext hoist() {
    return new ParquetConverterContext(parent, parent.fieldOnParent, type, repeated);
  }

  public void acceptCurrentValue() {
    accept(current.value());
  }

  public void accept(Object value) {
    parent.current.add(fieldOnParent, value, repeated);
  }

  public int getFieldCount() {
    return fieldCount;
  }

  public void accept(Object k, Object v) {
    parent.current.add(k, v, repeated);
  }

  public ParquetConverterContext repeated(boolean newRepeated) {
    return new ParquetConverterContext(parent, fieldOnParent, type, newRepeated);
  }

  public boolean named(String name) {
    return type.getName().equalsIgnoreCase(name);
  }

  boolean onlyField(String name) {
    return !type.isPrimitive() && fieldCount == 1 &&
      type.asGroupType().getFieldName(0).equalsIgnoreCase(name);
  }

  public Type type() {
    return type;
  }

  @Override
  public String toString() {
    return "Context[" +
      "parent=" + parent + ", " +
      "fieldOnParent=" + fieldOnParent + ", " +
      "type=" + type + ", " +
      "repeated=" + repeated + ']';
  }
}
