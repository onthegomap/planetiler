package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.util.ToDoubleFunctionThatThrows;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import systems.uom.common.USCustomary;

/** Units of length and area measurement based off of constants defined in {@link USCustomary}. */
public interface Unit {

  Pattern EXTRA_CHARS = Pattern.compile("[^a-z]+");
  Pattern TRAILING_S = Pattern.compile("s$");

  private static <T extends Unit> Map<String, T> index(T[] values) {
    return Arrays.stream(values)
      .flatMap(unit -> Stream.concat(unit.symbols().stream(), Stream.of(unit.unitName(), unit.toString()))
        .map(label -> Map.entry(normalize(label), unit))
        .distinct())
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  private static String normalize(String unit) {
    String result = EXTRA_CHARS.matcher(unit.toLowerCase()).replaceAll("");
    return TRAILING_S.matcher(result).replaceAll("");
  }

  /** The {@link Base} measurement this unit is based off of. */
  Base base();

  /** Computes the size of {@code geometry} in this unit. */
  double of(WithGeometry geometry);

  /** The aliases for this unit. */
  List<String> symbols();

  /** The full name for this unit. */
  String unitName();

  /** Converts a measurement in {@link Base} units to this unit. */
  double fromBaseUnit(double base);

  /** The base units that all other units are derived from. */
  enum Base {
    /** Size of a feature in "z0 tiles" where 1=the length/width/area entire world. */
    Z0_TILE(
      WithGeometry::length,
      WithGeometry::area),
    /** Size of a feature in meters. */
    METER(
      WithGeometry::lengthMeters,
      WithGeometry::areaMeters);

    private final ToDoubleFunctionThatThrows<WithGeometry> area;
    private final ToDoubleFunctionThatThrows<WithGeometry> length;

    Base(ToDoubleFunctionThatThrows<WithGeometry> length, ToDoubleFunctionThatThrows<WithGeometry> area) {
      this.length = length;
      this.area = area;
    }

    public double area(WithGeometry geometry) {
      return area.applyAndWrapException(geometry);
    }

    public double length(WithGeometry geometry) {
      return length.applyAndWrapException(geometry);
    }
  }

  /** Units to measure line length. */
  enum Length implements Unit {
    METER(USCustomary.METER, "m"),
    FOOT(USCustomary.FOOT, "ft", "feet"),
    YARD(USCustomary.YARD, "yd"),
    NAUTICAL_MILE(USCustomary.NAUTICAL_MILE, "nm"),
    MILE(USCustomary.MILE, "mi", "miles"),
    KILOMETER(Base.METER, 1e-3, List.of("km"), "Kilometer"),

    Z0_PIXEL(Base.Z0_TILE, 1d / 256, List.of("z0_px"), "Z0 Pixel"),
    Z0_TILE(Base.Z0_TILE, 1d, List.of("z0_ti"), "Z0 Tile");

    private static final Map<String, Length> NAMES = index(values());
    private final Base base;
    private final double multiplier;
    private final List<String> symbols;
    private final String name;

    Length(Base base, double multiplier, List<String> symbols, String name) {
      this.base = base;
      this.multiplier = multiplier;
      this.symbols = Stream.concat(symbols.stream(), Stream.of(name, name())).distinct().toList();
      this.name = name;
    }

    Length(javax.measure.Unit<javax.measure.quantity.Length> from, String... alias) {
      this(Base.METER, USCustomary.METER.getConverterTo(from).convert(1d), List.of(alias), from.getName());
    }

    public static Length from(String label) {
      Length unit = NAMES.get(normalize(label));
      if (unit == null) {
        throw new IllegalArgumentException("Could not find area unit for '%s'".formatted(label));
      }
      return unit;
    }

    @Override
    public double fromBaseUnit(double i) {
      return i * multiplier;
    }

    @Override
    public Base base() {
      return base;
    }

    @Override
    public double of(WithGeometry geometry) {
      return fromBaseUnit(base.length.applyAndWrapException(geometry));
    }

    @Override
    public List<String> symbols() {
      return symbols;
    }

    @Override
    public String unitName() {
      return name;
    }
  }

  /** Units to measure polygon areas. */
  enum Area implements Unit {
    SQUARE_METER(Length.METER),
    SQUARE_FOOT(Length.FOOT),
    SQUARE_YARD(Length.YARD),
    SQUARE_NAUTICAL_MILE(Length.NAUTICAL_MILE),
    SQUARE_MILE(Length.MILE),
    SQUARE_KILOMETER(Length.KILOMETER),

    SQUARE_Z0_PIXEL(Length.Z0_PIXEL),
    SQUARE_Z0_TILE(Length.Z0_TILE),

    ARE(USCustomary.ARE, "a"),
    HECTARE(USCustomary.HECTARE, "ha"),
    ACRE(USCustomary.ACRE, "ac");

    private static final Map<String, Area> NAMES = index(values());
    private final Base base;
    private final double multiplier;
    private final List<String> symbols;
    private final String name;

    Area(Base base, double multiplier, List<String> symbols, String name) {
      this.base = base;
      this.multiplier = multiplier;
      this.symbols = symbols;
      this.name = name;
    }

    Area(Length length) {
      this(length.base, length.multiplier * length.multiplier,
        length.symbols().stream().flatMap(symbol -> Stream.of(
          "s" + symbol,
          "square " + symbol,
          "sq " + symbol,
          symbol + "2"
        )).toList(),
        "Square " + length.name);
    }

    Area(javax.measure.Unit<javax.measure.quantity.Area> area, String... symbols) {
      this(Base.METER, USCustomary.ARE.getConverterTo(area).convert(0.01d), List.of(symbols), area.getName());
    }

    public static Area from(String label) {
      Area unit = NAMES.get(normalize(label));
      if (unit == null) {
        throw new IllegalArgumentException("Could not find area unit for '%s'".formatted(label));
      }
      return unit;
    }

    @Override
    public double fromBaseUnit(double base) {
      return base * multiplier;
    }

    @Override
    public Base base() {
      return base;
    }

    @Override
    public double of(WithGeometry geometry) {
      return fromBaseUnit(base.area.applyAndWrapException(geometry));
    }

    @Override
    public List<String> symbols() {
      return symbols;
    }

    @Override
    public String unitName() {
      return name;
    }
  }
}
