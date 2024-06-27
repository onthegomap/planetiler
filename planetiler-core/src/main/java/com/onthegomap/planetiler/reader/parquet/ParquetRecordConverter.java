package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.reader.FileFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.io.api.RecordMaterializer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

/**
 * Simple converter for parquet datatypes that maps all structs to {@code Map<String, Object>} and handles deserializing
 * <a href="https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#nested-types">list and map nested
 * types</a> into java {@link List Lists} and {@link Map Maps}.
 */
public class ParquetRecordConverter extends RecordMaterializer<Map<String, Object>> {

  private final StructConverter root;
  private Map<String, Object> map;

  ParquetRecordConverter(MessageType schema, GeoParquetMetadata geoParquetMetadata) {
    root = new StructConverter(new Context(schema, geoParquetMetadata)) {
      @Override
      public void start() {
        var group = new MapGroup(schema.getFieldCount());
        context.current = group;
        map = group.getMap();
      }
    };
    if (geoParquetMetadata != null) {
      validateGeometryColumn(schema, geoParquetMetadata);
    }
  }

  private void validateGeometryColumn(MessageType schema, GeoParquetMetadata geoParquetMetadata) {
    var primary = geoParquetMetadata.primaryColumnMetadata();
    String geoColumn = geoParquetMetadata.primaryColumn();
    var colSchema = schema.getType(geoColumn);
    var encoding = primary.encoding();
    switch (encoding) {
      case "WKT" -> require(
        colSchema.isPrimitive() &&
          colSchema.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY &&
          colSchema.getLogicalTypeAnnotation() == LogicalTypeAnnotation.stringType(),
        "String type required for wkt-encoded geometry column " + geoColumn + " got: " + colSchema);
      case "WKB" -> require(
        colSchema.isPrimitive() &&
          colSchema.asPrimitiveType().getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY &&
          colSchema.getLogicalTypeAnnotation() == null,
        "Binary type required for wkb-encoded geometry column " + geoColumn + " got: " + colSchema);
      case "point" ->
        requireConverter(encoding, geoColumn,
          GeoArrowCoordinateConverter.class, 0, "Coordinate");
      case "multipoint" ->
        requireConverter(encoding, geoColumn,
          GeoArrowCoordinateConverter.class, 1, "list<Coordinate>");
      case "linestring" ->
        requireConverter(encoding, geoColumn,
          GeoArrowCoordinateSequenceConverter.class, 0, "list<Coordinate>");
      case "multilinestring", "polygon" ->
        requireConverter(encoding, geoColumn,
          GeoArrowCoordinateSequenceConverter.class, 1, "list<list<Coordinate>>");
      case "multipolygon" ->
        requireConverter(encoding, geoColumn,
          GeoArrowCoordinateSequenceConverter.class, 2, "list<list<list<Coordinate>>>");
      case null, default -> throw new FileFormatException("Unexpected geoparquet geometry encoding: " + encoding);
    }
  }

  private void requireConverter(String encoding, String column,
    Class<?> clazz, int nesting, String pretty) {
    var colSchema = root.context.type.asGroupType().getType(column);
    var colIdx = root.context.type.asGroupType().getFieldIndex(column);
    Converter converter = root.converters[colIdx];
    for (int i = 0; i < nesting; i++) {
      converter = getListElement(converter);
    }
    require(
      clazz.isInstance(converter),
      pretty + " type required for geoarrow " + encoding + " column " + column + " got: " + colSchema);
  }

  private static Converter getListElement(Converter converter) {
    return (converter instanceof ListConverter lc && lc.getConverter(0) instanceof ListElementConverter lec) ?
      lec.getConverter(0) : null;
  }

  private static void require(boolean condition, String message) {
    if (!condition) {
      throw new FileFormatException(message);
    }
  }

  ParquetRecordConverter(MessageType schema) {
    this(schema, null);
  }

  @Override
  public Map<String, Object> getCurrentRecord() {
    return map;
  }

  @Override
  public void skipCurrentRecord() {
    root.context.current = null;
  }

  @Override
  public GroupConverter getRootConverter() {
    return root;
  }


  interface Group {
    // TODO handle repeated when processing schema, not elements
    void add(Object key, Object value, boolean repeated);

    Object value();
  }

  private static class ListConverter extends StructConverter {

    ListConverter(Context context) {
      super(context);
    }

    @Override
    protected Converter makeConverter(Context child) {
      if ((child.named("list") || child.named("array")) &&
        (child.onlyField("element") || child.onlyField("array_element"))) {
        return new ListElementConverter(child.hoist());
      }
      return super.makeConverter(child);
    }

    @Override
    public void start() {
      context.current = new ListGroup();
      context.acceptCurrentValue();
    }
  }

  private static class GeoArrowCoordinateSequenceConverter extends StructConverter {

    private final int dims;
    private CoordinateSequenceBuilder currentSequence;
    private int idx;

    GeoArrowCoordinateSequenceConverter(Context context) {
      super(context);
      this.dims = context.type.asGroupType()
        .getType(0).asGroupType()
        .getType(0).asGroupType()
        .getFieldCount();
    }

    @Override
    protected Converter makeConverter(Context child) {
      return new StructConverter(child) {

        @Override
        public void start() {
          // don't start a new struct
        }

        @Override
        protected Converter makeConverter(Context child) {
          return new StructConverter(child) {

            @Override
            public void start() {
              // don't start a new struct
            }

            @Override
            public void end() {
              idx++;
            }

            private PrimitiveConverter ordinateSetter(int ordinate) {
              return new PrimitiveConverter() {
                @Override
                public void addDouble(double value) {
                  currentSequence.setOrdinate(idx, ordinate, value);
                }
              };
            }

            @Override
            protected Converter makeConverter(Context child) {
              return switch (child.type.getName()) {
                case "x" -> ordinateSetter(0);
                case "y" -> ordinateSetter(1);
                case "z" -> ordinateSetter(2);
                case "m" -> ordinateSetter(3);
                default -> throw new IllegalStateException("Unexpected value: " + child.type.getName());
              };
            }
          };
        }
      };
    }

    @Override
    public void start() {
      idx = 0;
      currentSequence = new CoordinateSequenceBuilder(dims);
      context.accept(currentSequence);
    }
  }

  private static class GeoArrowCoordinateConverter extends StructConverter {

    private final int dims;
    private CoordinateSequenceBuilder currentSequence;

    GeoArrowCoordinateConverter(Context context) {
      super(context);
      this.dims = context.type.asGroupType().getFieldCount();
    }

    private PrimitiveConverter ordinateSetter(int ordinate) {
      return new PrimitiveConverter() {
        @Override
        public void addDouble(double value) {
          currentSequence.setOrdinate(0, ordinate, value);
        }
      };
    }

    @Override
    protected Converter makeConverter(Context child) {
      return switch (child.type.getName()) {
        case "x" -> ordinateSetter(0);
        case "y" -> ordinateSetter(1);
        case "z" -> ordinateSetter(2);
        case "m" -> ordinateSetter(3);
        default -> throw new IllegalStateException("Unexpected value: " + child.type.getName());
      };
    }

    @Override
    public void start() {
      currentSequence = new CoordinateSequenceBuilder(dims);
      context.accept(currentSequence);
    }
  }

  private static class ListElementConverter extends StructConverter {

    ListElementConverter(Context context) {
      super(context);
    }

    @Override
    public void start() {
      context.current = new ItemGroup();
    }

    @Override
    public void end() {
      context.acceptCurrentValue();
    }
  }

  private static class MapConverter extends StructConverter {

    MapConverter(Context context) {
      super(context);
    }

    @Override
    protected Converter makeConverter(Context child) {
      if (context.getFieldCount() == 1) {
        Type type = child.type;
        String onlyFieldName = type.getName().toLowerCase(Locale.ROOT);
        if (!type.isPrimitive() && type.asGroupType().getFieldCount() == 2 &&
          (onlyFieldName.equals("key_value") || onlyFieldName.equals("map"))) {
          return new MapEntryConverter(child.repeated(false));
        }
      }
      return super.makeConverter(child);
    }

    @Override
    public void start() {
      context.current = new MapGroup();
      context.acceptCurrentValue();
    }
  }

  private static class MapEntryConverter extends StructConverter {
    MapEntryGroup entry;

    MapEntryConverter(Context context) {
      super(context);
    }

    @Override
    public void start() {
      context.current = entry = new MapEntryGroup();
    }

    @Override
    public void end() {
      if (entry.v != null && entry.k != null) {
        context.accept(entry.k, entry.v);
      }
    }
  }

  static class StructConverter extends GroupConverter {

    final Context context;
    private final Converter[] converters;

    StructConverter(Context context) {
      this.context = context;
      int count = context.type.asGroupType().getFieldCount();
      converters = new Converter[count];
      for (int i = 0; i < count; i++) {
        converters[i] = makeConverter(context.field(i));
      }
    }

    protected Converter makeConverter(Context child) {
      Type type = child.type;
      LogicalTypeAnnotation logical = type.getLogicalTypeAnnotation();
      if (!type.isPrimitive()) {
        if (child.isGeoArrowCoordSeq()) {
          return new GeoArrowCoordinateSequenceConverter(child);
        } else if (child.isGeoArrowCoordinate()) {
          return new GeoArrowCoordinateConverter(child);
        }
        return switch (logical) {
          case LogicalTypeAnnotation.ListLogicalTypeAnnotation ignored ->
            // If the repeated field is not a group, then its type is the element type and elements are required.
            // If the repeated field is a group with multiple fields, then its type is the element type and elements are required.
            // If the repeated field is a group with one field and is named either array or uses the LIST-annotated group's name with _tuple appended then the repeated type is the element type and elements are required.
            // Otherwise, the repeated field's type is the element type with the repeated field's repetition.
            new ListConverter(child);
          case LogicalTypeAnnotation.MapLogicalTypeAnnotation ignored ->
            // The outer-most level must be a group annotated with MAP that contains a single field named key_value. The repetition of this level must be either optional or required and determines whether the list is nullable.
            // The middle level, named key_value, must be a repeated group with a key field for map keys and, optionally, a value field for map values.
            // The key field encodes the map's key type. This field must have repetition required and must always be present.
            // The value field encodes the map's value type and repetition. This field can be required, optional, or omitted.
            new MapConverter(child);
          case LogicalTypeAnnotation.MapKeyValueTypeAnnotation ignored ->
            new MapConverter(child);
          case null, default -> new StructConverter(child);
        };
      }
      return ParquetPrimitiveConverter.of(child);
    }

    @Override
    public Converter getConverter(int fieldIndex) {
      return converters[fieldIndex];
    }

    @Override
    public void start() {
      context.current = new MapGroup(context.getFieldCount());
      context.acceptCurrentValue();
    }

    @Override
    public void end() {
      // by default, don't need to do anything
    }
  }

  private static class MapGroup implements Group {

    private final Map<Object, Object> map;

    MapGroup() {
      this(10);
    }

    MapGroup(int size) {
      map = HashMap.newHashMap(size * 2);
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

  /** Constructs java objects from parquet records at read-time. */
  static final class Context {

    final Context parent;
    final String fieldOnParent;
    final Type type;
    final boolean repeated;
    private final int fieldCount;
    private GeoParquetMetadata metadata;
    Group current;

    Context(Context parent, String fieldOnParent, Type type, boolean repeated) {
      this.parent = parent;
      this.fieldOnParent = fieldOnParent;
      this.type = type;
      this.repeated = repeated;
      this.fieldCount = type.isPrimitive() ? 0 : type.asGroupType().getFieldCount();
    }

    public Context(Context newParent, Type type) {
      this(newParent, type, null);
    }

    public Context(Context newParent, Type type, GeoParquetMetadata metadata) {
      this(newParent, type.getName(), type, type.isRepetition(Type.Repetition.REPEATED));
      this.metadata = metadata;
    }

    public Context(MessageType schema, GeoParquetMetadata metadata) {
      this(null, schema, metadata);
    }

    public Context field(int i) {
      return new Context(this, type.asGroupType().getType(i));
    }

    /** Returns a new context that flattens-out this level of the hierarchy and writes values into the parent field. */
    public Context hoist() {
      return new Context(parent, parent.fieldOnParent, type, repeated);
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

    public Context repeated(boolean newRepeated) {
      return new Context(parent, fieldOnParent, type, newRepeated);
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

    public boolean isGeoArrowCoordSeq() {
      String geoArrowType = getGeoArrowType();
      if (geoArrowType == null || geoArrowType.contains("point")) {
        return false;
      }
      if (type.isPrimitive() || type.asGroupType().getFieldCount() != 1 ||
        type.getLogicalTypeAnnotation() != LogicalTypeAnnotation.listType()) {
        return false;
      }
      var repeatedElement = this.type.asGroupType().getType(0);
      if (!repeatedElement.isRepetition(Type.Repetition.REPEATED) || repeatedElement.isPrimitive() ||
        repeatedElement.asGroupType().getFieldCount() != 1) {
        return false;
      }
      return isGeoArrowCoordinate(repeatedElement.asGroupType().getType(0));
    }

    public boolean isGeoArrowCoordinate() {
      String geoArrowType = getGeoArrowType();
      return geoArrowType != null && geoArrowType.contains("point") && isGeoArrowCoordinate(type);
    }

    private String getGeoArrowType() {
      if (parent == null) {
        return null;
      } else if (parent.metadata != null) {
        var column = parent.metadata.columns().get(type.getName());
        return column == null ? null : column.getGeoArrowType();
      } else {
        return parent.getGeoArrowType();
      }
    }

    private static boolean isGeoArrowCoordinate(Type struct) {
      if (struct.isPrimitive()) {
        return false;
      }
      var group = struct.asGroupType();
      var names = group.getFields().stream().map(Type::getName).collect(Collectors.toSet());
      var types = group.getFields().stream()
        .map(d -> d.isPrimitive() ? d.asPrimitiveType().getPrimitiveTypeName() : null).collect(Collectors.toSet());
      return types.equals(Set.of(PrimitiveType.PrimitiveTypeName.DOUBLE)) &&
        (names.equals(Set.of("x", "y")) || names.equals(Set.of("x", "y", "z")) ||
          names.equals(Set.of("x", "y", "z", "m")));
    }
  }
}
