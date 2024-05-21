package com.onthegomap.planetiler.reader.parquet;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

/**
 * Simple converter for parquet datatypes that maps all structs to {@code Map<String, Object>} and handles deserializing
 * <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types">list and map nested
 * types</a> into java {@link List Lists} and {@link Map Maps}.
 */
public class ParquetRecordConverter extends RecordMaterializer<Map<String, Object>> {

  private final StructConverter root;
  private Map<String, Object> map;

  ParquetRecordConverter(MessageType schema) {
    root = new StructConverter(null, schema) {
      @Override
      public void start() {
        var group = new MapGroup(schema.getFieldCount());
        this.current = group;
        map = group.getMap();
      }
    };
  }

  @Override
  public Map<String, Object> getCurrentRecord() {
    return map;
  }

  @Override
  public void skipCurrentRecord() {
    root.current = null;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }


  private static class ListConverter extends StructConverter {

    ListConverter(StructConverter parent, String fieldOnParent, GroupType schema) {
      super(parent, schema, fieldOnParent, schema.isRepetition(Type.Repetition.REPEATED));
    }

    ListConverter(StructConverter parent, GroupType schema) {
      this(parent, schema.getName(), schema);
    }

    private boolean onlyField(Type type, String name) {
      return !type.isPrimitive() && type.asGroupType().getFieldCount() == 1 &&
        type.asGroupType().getFieldName(0).equalsIgnoreCase(name);
    }

    @Override
    protected Converter makeConverter(int fieldIdx, String fieldOnParent) {
      if (schema.getFieldCount() == 1) {
        Type type = schema.getType(0);
        if ((type.getName().equalsIgnoreCase("list") || type.getName().equalsIgnoreCase("array")) &&
          onlyField(type, "element")) {
          return new ListElementConverter(this, this.fieldOnParent, type.asGroupType());
        }
      }
      return super.makeConverter(fieldIdx, fieldOnParent);
    }

    @Override
    public void start() {
      this.current = new ListGroup();
      parent.current.add(this.fieldOnParent, current.value(), repeated);
    }
  }


  private static class ListElementConverter extends StructConverter {

    ListElementConverter(StructConverter parent, String fieldOnParent, GroupType schema) {
      super(parent, schema, fieldOnParent, true);
    }

    @Override
    public void start() {
      this.current = new ItemGroup();
    }

    @Override
    public void end() {
      parent.current.add(this.fieldOnParent, current.value(), parent.repeated);
    }
  }

  private static class MapConverter extends StructConverter {

    MapConverter(StructConverter parent, String fieldOnParent, GroupType schema) {
      super(parent, schema, fieldOnParent, schema.isRepetition(Type.Repetition.REPEATED));
    }

    @Override
    protected Converter makeConverter(int fieldIdx, String fieldOnParent) {
      if (schema.getFieldCount() == 1) {
        Type type = schema.getType(fieldIdx);
        String onlyFieldName = type.getName().toLowerCase(Locale.ROOT);
        if (!type.isPrimitive() && type.asGroupType().getFieldCount() == 2 &&
          (onlyFieldName.equals("key_value") || onlyFieldName.equals("map"))) {
          return new MapEntryConverter(this, fieldOnParent, type.asGroupType());
        }
      }
      return super.makeConverter(fieldIdx, fieldOnParent);
    }

    @Override
    public void start() {
      this.current = new MapGroup();
      parent.current.add(this.fieldOnParent, current.value(), repeated);
    }
  }

  private static class MapEntryConverter extends StructConverter {
    MapEntryGroup entry;

    MapEntryConverter(StructConverter parent, String fieldOnParent, GroupType schema) {
      super(parent, schema, fieldOnParent, true);
    }

    @Override
    public void start() {
      current = entry = new MapEntryGroup();
    }

    @Override
    public void end() {
      if (entry.v != null) {
        parent.current.add(entry.k, entry.v, false);
      }
    }
  }


  static class StructConverter extends GroupConverter {

    final StructConverter parent;
    final boolean repeated;
    final GroupType schema;
    final String fieldOnParent;
    Group current;
    private final Converter[] converters;

    StructConverter(StructConverter parent, GroupType schema) {
      this(parent, schema, schema.getName(), schema.isRepetition(Type.Repetition.REPEATED));
    }

    StructConverter(StructConverter parent, GroupType schema, String fieldOnParent, boolean repeated) {
      this.parent = parent;
      this.schema = schema;
      this.repeated = repeated;
      this.fieldOnParent = fieldOnParent;
      converters = IntStream.range(0, schema.getFieldCount()).mapToObj(this::makeConverter).toArray(Converter[]::new);
    }

    protected Converter makeConverter(int fieldIdx) {
      return makeConverter(fieldIdx, schema.getFieldName(fieldIdx));
    }

    protected Converter makeConverter(int fieldIdx, String fieldOnParent) {
      return makeConverter(fieldIdx, fieldOnParent, schema.getType(fieldIdx).isRepetition(Type.Repetition.REPEATED));
    }

    protected Converter makeConverter(int fieldIdx, String fieldOnParent, boolean repeated) {
      Type type = schema.getType(fieldIdx);
      LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
      if (!type.isPrimitive()) {
        return switch (logical) {
          case LogicalTypeAnnotation.ListLogicalTypeAnnotation list ->
            // If the repeated field is not a group, then its type is the element type and elements are required.
            // If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
            // If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name with _tuple appended then the repeated type is the element type and elements are required.
            // Otherwise, the repeated field's type is the element type with the repeated field's repetition.
            new ListConverter(this, type.asGroupType());
          case LogicalTypeAnnotation.MapLogicalTypeAnnotation m ->
            // The outer-most level must be a group annotated with MAP that contains a single field named key_value. The repetition of this level must be either optional or required and determines whether the list is nullable.
            // The middle level, named key_value, must be a repeated group with a key field for map keys and, optionally, a value field for map values.
            // The key field encodes the map's key type. This field must have repetition required and must always be present.
            // The value field encodes the map's value type and repetition. This field can be required, optional, or omitted.
            new MapConverter(this, fieldOnParent, type.asGroupType());
          case LogicalTypeAnnotation.MapKeyValueTypeAnnotation m ->
            new MapConverter(this, fieldOnParent, type.asGroupType());
          case null, default -> new StructConverter(this, type.asGroupType());
        };
      }
      return ParquetPrimitiveConverter.of(new Context(this, fieldOnParent, type, repeated));
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      current = new MapGroup(schema.getFieldCount());
      parent.current.add(schema.getName(), current.value(), repeated);
    }

    @Override
    public void end() {
      // don't need to do anything
    }
  }

  interface Group {
    // TODO handle repeated when processing schema, not elements
    void add(Object key, Object value, boolean repeated);

    Object value();
  }

  private static class MapGroup implements Group {

    private final Map<Object, Object> map;

    MapGroup() {
      map = new HashMap<>();
    }

    MapGroup(int size) {
      map = HashMap.newHashMap(size);
    }

    @Override
    public void add(Object key, Object value, boolean repeated) {
      if (repeated) {
        List<Object> items = (List<Object>) map.computeIfAbsent(key, n -> new ArrayList<>());
        items.add(value);
      } else {
        if (map.put(key, value) != null) {
          throw new IllegalStateException("Multiple values for " + key);
        }
      }
    }

    @Override
    public String toString() {
      return "MapGroup" + map;
    }

    public Map<String, Object> getMap() {
      return (Map<String, Object>) (Map<?, Object>) map;
    }

    @Override
    public Object value() {
      return map;
    }
  }

  private static class ListGroup implements Group {

    private final List<Object> list = new ArrayList<>();

    @Override
    public void add(Object key, Object value, boolean repeated) {
      list.add(value);
    }

    @Override
    public String toString() {
      return "ListGroup" + list;
    }

    @Override
    public Object value() {
      return list;
    }
  }

  private static class ItemGroup implements Group {

    private Object item;

    @Override
    public void add(Object key, Object value, boolean repeated) {
      if (repeated) {
        if (item == null) {
          item = new ArrayList<>();
        }
        ((List<Object>) item).add(value);
      } else {
        item = value;
      }
    }

    @Override
    public String toString() {
      return "ItemGroup{" + item + '}';
    }

    @Override
    public Object value() {
      return item;
    }
  }

  private static class MapEntryGroup implements Group {

    private Object k;
    private Object v;

    @Override
    public void add(Object key, Object value, boolean repeated) {
      if ("key".equals(key)) {
        k = value;
      } else if ("value".equals(key)) {
        v = value;
      } else if (k == null) {
        k = value;
      } else {
        v = value;
      }
    }

    @Override
    public String toString() {
      return "MapEntryGroup{" + k + '=' + v + '}';
    }

    @Override
    public Object value() {
      throw new UnsupportedOperationException();
    }
  }

  record Context(
    ParquetRecordConverter.StructConverter parent,
    String fieldOnParent,
    Type type,
    boolean repeated
  ) {}
}
