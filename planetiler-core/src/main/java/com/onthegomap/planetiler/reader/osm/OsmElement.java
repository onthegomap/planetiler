package com.onthegomap.planetiler.reader.osm;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * An input element read from OpenStreetMap data.
 *
 * @see <a href="https://wiki.openstreetmap.org/wiki/Elements">OSM element data model</a>
 */
public interface OsmElement extends WithTags {

  /** OSM element ID */
  long id();

  enum Type {
    NODE, WAY, RELATION
  }

  /** An un-handled element read from the .osm.pbf file (i.e. file header). */
  record Other(
    @Override long id,
    @Override Map<String, Object> tags
  ) implements OsmElement {}

  /** A point on the earth's surface. */
  final class Node implements OsmElement {

    private static final long MISSING_LOCATION = Long.MIN_VALUE;
    private final long id;
    private final Map<String, Object> tags;
    private final double lat;
    private final double lon;
    // bailed out of a record to make encodedLocation lazy since it is fairly expensive to compute
    private long encodedLocation = MISSING_LOCATION;

    public Node(
      long id,
      Map<String, Object> tags,
      double lat,
      double lon
    ) {
      this.id = id;
      this.tags = tags;
      this.lat = lat;
      this.lon = lon;
    }

    public Node(long id, double lat, double lon) {
      this(id, new HashMap<>(), lat, lon);
    }

    @Override
    public long id() {
      return id;
    }

    @Override
    public Map<String, Object> tags() {
      return tags;
    }

    public double lat() {
      return lat;
    }

    public double lon() {
      return lon;
    }

    public long encodedLocation() {
      if (encodedLocation == MISSING_LOCATION) {
        encodedLocation = GeoUtils.encodeFlatLocation(lon, lat);
      }
      return encodedLocation;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != this.getClass()) {
        return false;
      }
      var that = (Node) obj;
      return this.id == that.id &&
        Objects.equals(this.tags, that.tags) &&
        Double.doubleToLongBits(this.lat) == Double.doubleToLongBits(that.lat) &&
        Double.doubleToLongBits(this.lon) == Double.doubleToLongBits(that.lon);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, tags, lat, lon);
    }

    @Override
    public String toString() {
      return "Node[" +
        "id=" + id + ", " +
        "tags=" + tags + ", " +
        "lat=" + lat + ", " +
        "lon=" + lon + ']';
    }

  }

  /** An ordered list of 2-2,000 nodes that define a polyline. */
  record Way(
    @Override long id,
    @Override Map<String, Object> tags,
    LongArrayList nodes
  ) implements OsmElement {

    public Way(long id) {
      this(id, new HashMap<>(), new LongArrayList(5));
    }
  }

  /** An ordered list of nodes, ways, and other relations. */
  record Relation(
    @Override long id,
    @Override Map<String, Object> tags,
    List<Member> members
  ) implements OsmElement {

    public Relation(long id) {
      this(id, new HashMap<>(), new ArrayList<>());
    }

    public Relation {
      if (members == null) {
        members = Collections.emptyList();
      }
    }

    /** A node, way, or relation contained in a relation with an optional "role" to clarify the purpose of each member. */
    public record Member(
      Type type,
      long ref,
      String role
    ) {}
  }
}
