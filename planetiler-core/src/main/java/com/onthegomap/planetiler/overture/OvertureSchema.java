package com.onthegomap.planetiler.overture;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

public class OvertureSchema {
  static final ObjectMapper mapper = new ObjectMapper();

  public record AdministrativeBoundary(
    @Override SharedMetadata info,
    int adminLevel,
    boolean maritime
  ) {

    public static AdministrativeBoundary parse(GenericRecord record) {
      return new AdministrativeBoundary(
        SharedMetadata.parse(record),
        parseInt(record.get("adminlevel")),
        parseBool(record.get("maritime"))
      );
    }
  }

  public record Locality(
    @Override SharedMetadata info,
    String subType,
    String localityType,
    Names names,
    Object context,
    Integer adminLevel,
    String isoCountryCodeAlpha2,
    String isoSubCountryCode,
    String defaultLanguage,
    DrivingSide drivingSide
  ) {

    public static Locality parse(GenericRecord record) {
      return new Locality(
        SharedMetadata.parse(record),
        record.get("subtype").toString(),
        record.get("localitytype").toString(),
        Names.parse(record.get("names")),
        record.get("context"),
        parseInt(record.get("adminlevel")),
        stringify(record.get("isocountrycodealpha2")),
        stringify(record.get("isosubcountrycode")),
        stringify(record.get("defaultlanugage")), // bug
        DrivingSide.parse(record.get("drivingside"))
      );
    }
  }

  public record Names(
    List<Name> common,
    List<Name> official,
    List<Name> alternate,
    List<Name> shortName,
    Range at
  ) {

    public static Names parse(Object obj) {
      return obj instanceof Map<?, ?> map ? new Names(
        parseList(map.get("common"), Name::parse),
        parseList(map.get("official"), Name::parse),
        parseList(map.get("alternate"), Name::parse),
        parseList(map.get("short"), Name::parse),
        null
      ) : null;
    }
  }

  public enum BuildingClass {
    RESIDENTIAL,
    OUTBUILDING,
    AGRICULTURAL,
    COMMERCIAL,
    INDUSTRIAL,
    EDUCATION,
    SERVICE,
    RELIGIOUS,
    CIVIC,
    TRANSPORTATION,
    MEDICAL,
    ENTERTAINMENT,
    MILITARY;

    public static BuildingClass parse(Object obj) {
      return obj == null ? null : BuildingClass.valueOf(obj.toString().toUpperCase());
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }

  public enum DrivingSide {
    LEFT,
    RIGHT;

    public static DrivingSide parse(Object str) {
      return str == null ? null : DrivingSide.valueOf(str.toString().toUpperCase());
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }

  public record Name(String value, String language) {

    public static Name parse(Object o) {
      return o instanceof Map<?, ?> map ? new Name(
        map.get("value").toString(),
        map.get("language").toString()
      ) : null;
    }
  }

  public record Building(
    @Override SharedMetadata info,
    Names names,
    Double height,
    Integer numFloors,
    BuildingClass class_,
    Integer zOrder
  ) {

    public static Building parse(GenericRecord record) {
      return new Building(
        SharedMetadata.parse(record),
        Names.parse(record.get("names")),
        parseDouble(record.get("height")),
        parseInt(record.get("numfloors")),
        BuildingClass.parse(record.get("class")),
        0 // TODO overture bug: no zorder
      //        parseIntOrDefault(record.get("zorder"), 0)
      );
    }
  }

  public record Place(
    @Override SharedMetadata info,
    Names names,
    Categories categories,
    Double confidence,
    List<String> websites,
    List<String> socials,
    List<String> emails,
    List<String> phones,
    Brand brand,
    List<Address> addresses
  ) {

    public static Place parse(GenericRecord record) {
      return new Place(
        SharedMetadata.parse(record),
        Names.parse(record.get("names")),
        Categories.parse(record.get("categories")),
        //        parseList(record.get("categories"), Categories::parse),
        parseDouble(record.get("confidence")),
        parseList(record.get("websites"), OvertureSchema::stringify),
        parseList(record.get("socials"), OvertureSchema::stringify),
        parseList(record.get("emails"), OvertureSchema::stringify),
        parseList(record.get("phones"), OvertureSchema::stringify),
        Brand.parse(record.get("brand")),
        parseList(record.get("addresses"), Address::parse)
      );
    }
  }

  record Brand(
    Names names,
    String wikidata
  ) {

    public static Brand parse(Object obj) {
      return obj instanceof GenericRecord record ? new Brand(
        Names.parse(record.get("names")),
        stringify(record.get("wikidata"))
      ) : null;
    }
  }

  record Address(
    String freeform,
    String locality,
    String postCode,
    String region,
    String country
  ) {

    public static Address parse(Object o) {
      return o instanceof GenericRecord record ? new Address(
        stringify(record.get("freeform")),
        stringify(record.get("locality")),
        stringify(record.get("postcode")),
        stringify(record.get("region")),
        stringify(record.get("country"))
      ) : null;
    }
  }

  record Categories(String main, List<String> alternate) {

    public static Categories parse(Object obj) {
      return obj instanceof GenericRecord record ? new Categories(
        stringify(record.get("main")),
        parseList(record.get("alternate"), OvertureSchema::stringify)
      ) : null;
    }
  }

  public record Connector(
    @Override SharedMetadata info,
    int zOrder
  ) {

    public static Connector parse(GenericRecord record) {
      return new Connector(
        SharedMetadata.parse(record),
        0
      // TODO overture bug: some timestamps are strings, some are INT96
      //parseIntOrDefault(record.get("zorder"), 0)
      );
    }
  }

  public record Segment(
    @Override SharedMetadata info,
    SegmentSubType subType,
    List<String> connectors,
    Double width,
    int zOrder,
    Road road
  ) implements OvertureObject {

    public static Segment parse(GenericRecord record) {
      return new Segment(
        SharedMetadata.parse(record),
        SegmentSubType.parse(record.get("subtype")),
        parseList(record.get("connectors"), OvertureSchema::stringify),
        null, // TODO overture bug no width: parseDouble(record.get("width")),
        0, // TODO overture bug no width: parseIntOrDefault(record.get("zorder"), 0),
        Road.parse(record.get("road"))
      );
    }
  }

  public enum SegmentSubType {
    @JsonProperty("road")
    ROAD,
    @JsonProperty("rail")
    RAIL,
    @JsonProperty("water")
    WATER;

    public static SegmentSubType parse(Object obj) {
      return obj == null ? null : SegmentSubType.valueOf(obj.toString().toUpperCase());
    }

    @Override
    public String toString() {
      return super.toString().toLowerCase();
    }
  }

  record Partial<T> (List<T> value, Range at) {
    public static <T> Partial<T> parse(Object obj, Class<T> innerClass) {
      return obj instanceof Map<?, ?> map ? new Partial<>(
        map.get("value")instanceof String s ?
          List.of(mapper.convertValue(s, innerClass)) :
          ((List<?>) map.get("value")).stream().map(item -> mapper.convertValue(item, innerClass)).toList(),
        mapper.convertValue(map.get("at"), Range.class)
      ) : obj instanceof String ? new Partial<>(List.of(mapper.convertValue(obj, innerClass)), null) : null;
    }
  }

  public record Road(
    @JsonProperty("class") RoadClass roadClass,
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY) List<Names> roadNames,
    @JsonFormat(with = JsonFormat.Feature.ACCEPT_SINGLE_VALUE_AS_ARRAY)
    // TODO break up road into partials
    @JsonProperty("surface") List<Object> surface,
    @JsonProperty("flags") List<Object> flags,
    @JsonProperty("lanes") List<Object> lanes,
    Object restrictions
  ) {
    //    public List<RoadSurface> surface() {
    //      return rawSurface == null ? List.of() : rawSurface.stream()
    //        .filter(String.class::isInstance)
    //        .map(s -> RoadSurface.valueOf(s.toString().toUpperCase()))
    //        .toList();
    //    }
    //
    //    public List<Partial<RoadSurface>> partialSurface() {
    //      return rawSurface == null ? List.of() : rawSurface.stream()
    //        .map(o -> Partial.parse(o, RoadSurface.class))
    //        .toList();
    //    }
    //
    //    public List<RoadFlags> flags() {
    //      return rawFlags == null ? List.of() : rawFlags.stream()
    //        .filter(String.class::isInstance)
    //        .map(s -> RoadFlags.valueOf(s.toString().toUpperCase()))
    //        .toList();
    //    }
    //
    //    public List<Partial<RoadFlags>> partialFlags() {
    //      return rawFlags == null ? List.of() : rawFlags.stream()
    //        .map(o -> Partial.parse(o, RoadFlags.class))
    //        .toList();
    //    }

    public static Road parse(Object road) {
      // TODO overture bug: road is just a json string
      try {
        return mapper.readValue(road.toString(), Road.class);
      } catch (JsonProcessingException e) {
        throw new UncheckedIOException(e);
      }
    }
  }

  //  public record RoadLane(
  //    Direction direction,
  //    RoadLaneRestrictions restrictions
  //  ) {}

  public enum Direction {
    @JsonProperty("forward")
    FORWARD("forward"),
    @JsonProperty("backward")
    BACKWARD("backward"),
    @JsonProperty("bothWays")
    BOTHWAYS("bothWays"),
    @JsonProperty("alternating")
    ALTERNATING("alternating"),
    @JsonProperty("reversible")
    REVERSIBLE("reversible");

    private final String value;

    Direction(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  //  public record RoadRestrictions(
  //    List<SpeedLimit> speedLimits
  //  //    String
  //  ) {}

  @JsonFormat(shape = JsonFormat.Shape.ARRAY)
  @JsonPropertyOrder({"from", "to"})
  record Range(double from, double to) {}

  public enum TransportationMode {
    @JsonProperty("motorVehicle")
    MOTORVEHICLE("motorVehicle"),
    @JsonProperty("car")
    CAR("car"),
    @JsonProperty("truck")
    TRUCK("truck"),
    @JsonProperty("motorcycle")
    MOTORCYCLE("motorcycle"),
    @JsonProperty("foot")
    FOOT("foot"),
    @JsonProperty("bicycle")
    BICYCLE("bicycle"),
    @JsonProperty("bus")
    BUS("bus"),
    @JsonProperty("hgv")
    HGV("hgv"),
    @JsonProperty("hov")
    HOV("hov"),
    @JsonProperty("emergency")
    EMERGENCY("emergency");

    private final String value;

    TransportationMode(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public record SpeedLimit(
    Speed minSpeed,
    Speed maxSpeed,
    Boolean isMaxSpeedVariable,
    Range at,
    String during,
    List<TransportationMode> mode,
    List<TransportationMode> modeNot
  ) {}

  public enum SpeedUnit {
    @JsonProperty("km/h")
    KMKH("km/h"),
    @JsonProperty("mph")
    MPH("mph");

    private final String value;

    SpeedUnit(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  @JsonFormat(shape = JsonFormat.Shape.ARRAY)
  @JsonPropertyOrder({"speed", "unit"})
  public record Speed(Integer speed, SpeedUnit unit) {}

  public enum RoadFlags {
    @JsonProperty("isBridge")
    ISBRIDGE("isBridge"),
    @JsonProperty("isLink")
    ISLINK("isLink"),
    @JsonProperty("isPrivate")
    ISPRIVATE("isPrivate"),
    @JsonProperty("isTunnel")
    ISTUNNEL("isTunnel"),
    @JsonProperty("isUnderConstruction")
    ISUNDERCONSTRUCTION("isUnderConstruction");

    private final String value;

    RoadFlags(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum RoadSurface {
    @JsonProperty("unknown")
    UNKNOWN("unknown"),
    @JsonProperty("paved")
    PAVED("paved"),
    @JsonProperty("unpaved")
    UNPAVED("unpaved"),
    @JsonProperty("gravel")
    GRAVEL("gravel"),
    @JsonProperty("dirt")
    DIRT("dirt"),
    @JsonProperty("pavingStones")
    PAVINGSTONES("pavingStones"),
    @JsonProperty("metal")
    METAL("metal");

    private final String value;

    RoadSurface(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public enum RoadClass {
    @JsonProperty("motorway")
    MOTORWAY("motorway"),
    @JsonProperty("primary")
    PRIMARY("primary"),
    @JsonProperty("secondary")
    SECONDARY("secondary"),
    @JsonProperty("tertiary")
    TERTIARY("tertiary"),
    @JsonProperty("residential")
    RESIDENTIAL("residential"),
    @JsonProperty("livingStreet")
    LIVINGSTREET("livingStreet"),
    @JsonProperty("trunk")
    TRUNK("trunk"),
    @JsonProperty("unclassified")
    UNCLASSIFIED("unclassified"),
    @JsonProperty("parkingAisle")
    PARKINGAISLE("parkingAisle"),
    @JsonProperty("driveway")
    DRIVEWAY("driveway"),
    @JsonProperty("pedestrian")
    PEDESTRIAN("pedestrian"),
    @JsonProperty("footway")
    FOOTWAY("footway"),
    @JsonProperty("steps")
    STEPS("steps"),
    @JsonProperty("track")
    TRACK("track"),
    @JsonProperty("cycleway")
    CYCLEWAY("cycleway"),
    @JsonProperty("bridleway")
    BRIDLEWAY("bridleway"),
    @JsonProperty("unknown")
    UNKNOWN("unknown");

    private final String value;

    RoadClass(String value) {
      this.value = value;
    }

    @Override
    public String toString() {
      return value;
    }
  }

  public interface OvertureObject {

    SharedMetadata info();
  }

  public record SharedMetadata(
    String id,
    int version,
    long updateTime,
    List<Source> sources
  ) {

    public static SharedMetadata parse(GenericRecord record) {
      return new SharedMetadata(
        stringify(record.get("id")),
        parseInt(record.get("version")),
        parseTimestamp(record.get("updatetime")),
        parseList(record.get("sources"), Source::parse)
      );
    }
  }

  private static Double parseDouble(Object obj) {
    return obj instanceof Double d ? d : obj instanceof Number n ? n.doubleValue() : null;
  }

  private static Integer parseInt(Object obj) {
    return obj instanceof Integer d ? d : obj instanceof Number n ? n.intValue() : null;
  }

  private static boolean parseBool(Object obj) {
    return obj instanceof Boolean b && b;
  }

  private static Integer parseIntOrDefault(Object obj, int defaultValue) {
    return obj instanceof Number n ? n.intValue() : defaultValue;
  }

  private static <T> List<T> parseList(Object obj, Function<Object, T> parseElement) {
    if (!(obj instanceof GenericData.Array<?> array)) {
      return List.of();
    }
    List<T> result = new ArrayList<>(array.size());
    for (Object element : array) {
      var item = parseElement.apply(((GenericRecord) element).get("array_element"));
      if (item != null) {
        result.add(item);
      }
    }
    return result;
  }

  private static long parseTimestamp(Object obj) {
    // TODO overture bug: some timestamps are strings, some are INT96
    if (obj instanceof GenericData.Fixed fixed) {
      ByteBuffer buf = ByteBuffer.wrap(fixed.bytes()).order(ByteOrder.LITTLE_ENDIAN);
      long timeOfDayNanos = buf.getLong();
      int julianDay = buf.getInt();
      long nanosecondsSinceUnixEpoch = (julianDay - 2440588) * (86400L * 1000 * 1000 * 1000) + timeOfDayNanos;
      return Duration.ofNanos(nanosecondsSinceUnixEpoch).toMillis();
    } else if (obj instanceof String s) {
      return Instant.parse(s + (s.endsWith("Z") ? "" : "Z")).toEpochMilli();
    }
    throw new IllegalArgumentException("Unknown timestamp type " + obj.getClass());
  }

  public record Source(String dataset, String property, String recordId) {

    public static Source parse(Object record) {
      return !(record instanceof Map<?, ?> map) ? null : new Source(
        stringify(map.get("dataset")),
        stringify(map.get("property")),
        // TODO overture bug: inconsistent casing
        stringify(map.get("recordid"), stringify(map.get("recordId"))));
    }
  }

  private static String stringify(Object o) {
    return o == null ? null : o.toString();
  }

  private static String stringify(Object o1, Object o2) {
    return o1 != null ? o1.toString() : o2 != null ? o2.toString() : null;
  }
}
