/*****************************************************************
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ****************************************************************/
package com.onthegomap.flatmap.read;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongObjectMap;
import com.carrotsearch.hppc.ObjectIntMap;
import com.carrotsearch.hppc.cursors.LongObjectCursor;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.graphhopper.coll.GHObjectIntHashMap;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedPolygon;

/**
 * This class is ported to Java from https://github.com/omniscale/imposm3/blob/master/geom/multipolygon.go and
 * https://github.com/omniscale/imposm3/blob/master/geom/ring.go
 */
public class OsmMultipolygon {

  private static final double MIN_CLOSE_RING_GAP = 0.1 / GeoUtils.WORLD_CIRCUMFERENCE_METERS;
  private static final Comparator<Ring> BY_AREA_DESCENDING = Comparator.comparingDouble(ring -> -ring.area);

  private static class Ring {

    private final Polygon geom;
    private final double area;
    private Ring containedBy = null;
    private final Set<Ring> holes = new HashSet<>();

    private Ring(Polygon geom) {
      this.geom = geom;
      this.area = geom.getArea();
    }

    public Polygon toPolygon() {
      return GeoUtils.JTS_FACTORY.createPolygon(
        geom.getExteriorRing(),
        holes.stream().map(ring -> ring.geom.getExteriorRing()).toArray(LinearRing[]::new)
      );
    }

    public boolean isHole() {
      int containedCounter = 0;
      for (Ring ring = this; ring != null; ring = ring.containedBy) {
        containedCounter++;
      }
      return containedCounter % 2 == 0;
    }
  }

  public static Geometry build(List<CoordinateSequence> rings) throws GeometryException {
    ObjectIntMap<Coordinate> coordToId = new GHObjectIntHashMap<>();
    List<Coordinate> idToCoord = new ArrayList<>();
    int id = 0;
    List<LongArrayList> idRings = new ArrayList<>(rings.size());
    for (CoordinateSequence coords : rings) {
      LongArrayList idRing = new LongArrayList(coords.size());
      idRings.add(idRing);
      for (Coordinate coord : coords.toCoordinateArray()) {
        if (!coordToId.containsKey(coord)) {
          coordToId.put(coord, id);
          idToCoord.add(coord);
          id++;
        }
        idRing.add(coordToId.get(coord));
      }
    }
    return build(idRings, lookupId -> idToCoord.get((int) lookupId), 0, MIN_CLOSE_RING_GAP);
  }

  public static Geometry build(
    List<LongArrayList> rings,
    OpenStreetMapReader.NodeLocationProvider nodeCache,
    long osmId
  ) throws GeometryException {
    return build(rings, nodeCache, osmId, MIN_CLOSE_RING_GAP);
  }

  public static Geometry build(
    List<LongArrayList> rings,
    OpenStreetMapReader.NodeLocationProvider nodeCache,
    long osmId,
    double minGap
  ) throws GeometryException {
    return build(rings, nodeCache, osmId, minGap, false);
  }

  public static Geometry build(
    List<LongArrayList> rings,
    OpenStreetMapReader.NodeLocationProvider nodeCache,
    long osmId,
    double minGap,
    boolean fix
  ) throws GeometryException {
    try {
      if (rings.size() == 0) {
        throw new GeometryException.Verbose("osm_invalid_multipolygon_empty",
          "error building multipolygon " + osmId + ": no rings to process");
      }
      List<LongArrayList> idSegments = connectPolygonSegments(rings);
      List<Ring> polygons = new ArrayList<>(idSegments.size());
      for (LongArrayList segment : idSegments) {
        int size = segment.size();
        long firstId = segment.get(0), lastId = segment.get(size - 1);
        if (firstId == lastId || tryClose(segment, nodeCache, minGap)) {
          CoordinateSequence coordinates = nodeCache.getWayGeometry(segment);
          Polygon poly = GeoUtils.JTS_FACTORY.createPolygon(coordinates);
          if (fix) {
            poly = (Polygon) GeoUtils.fixPolygon(poly);
          }
          polygons.add(new Ring(poly));
        }
      }
      polygons.sort(BY_AREA_DESCENDING);
      Set<Ring> shells = groupParentChildShells(polygons);
      if (shells.size() == 0) {
        throw new GeometryException.Verbose("osm_invalid_multipolygon_not_closed",
          "error building multipolygon " + osmId + ": multipolygon not closed");
      } else if (shells.size() == 1) {
        return shells.iterator().next().toPolygon();
      } else {
        Polygon[] finished = shells.stream().map(Ring::toPolygon).toArray(Polygon[]::new);
        return GeoUtils.JTS_FACTORY.createMultiPolygon(finished);
      }
    } catch (IllegalArgumentException e) {
      throw new GeometryException("osm_invalid_multipolygon", "error building multipolygon " + osmId + ": " + e);
    } catch (TopologyException e) {
      if (!fix) {
        // retry but fix every polygon first
        System.err.println("FIXING!");
        return build(rings, nodeCache, osmId, minGap, true);
      } else {
        throw new GeometryException("osm_invalid_multipolygon", "error building multipolygon " + osmId + ": " + e);
      }
    }
  }

  private static Set<Ring> groupParentChildShells(List<Ring> polygons) {
    Set<Ring> shells = new HashSet<>();
    int numPolygons = polygons.size();
    if (numPolygons == 0) {
      return shells;
    }
    shells.add(polygons.get(0));
    if (numPolygons == 1) {
      return shells;
    }
    for (int i = 0; i < numPolygons; i++) {
      Ring outer = polygons.get(i);
      if (i < numPolygons - 1) {
        PreparedPolygon prepared = new PreparedPolygon(outer.geom);
        for (int j = i + 1; j < numPolygons; j++) {
          Ring inner = polygons.get(j);
          if (prepared.contains(inner.geom)) {
            if (inner.containedBy != null) {
              inner.containedBy.holes.remove(inner);
              shells.remove(inner);
            }
            inner.containedBy = outer;
            if (inner.isHole()) {
              outer.holes.add(inner);
            } else {
              shells.add(inner);
            }
          }
        }
      }
      if (outer.containedBy == null) {
        shells.add(outer);
      }
    }
    return shells;
  }

  private static boolean tryClose(LongArrayList segment, OpenStreetMapReader.NodeLocationProvider nodeCache,
    double minGap) {
    int size = segment.size();
    long firstId = segment.get(0);
    Coordinate firstCoord = nodeCache.getCoordinate(firstId);
    Coordinate lastCoord = nodeCache.getCoordinate(segment.get(size - 1));
    if (firstCoord.distance(lastCoord) <= minGap) {
      segment.set(size - 1, firstId);
      return true;
    }
    return false;
  }

  private static void reverseInPlace(LongArrayList orig) {
    for (int i = 0, j = orig.size() - 1; i < j; i++, j--) {
      long temp = orig.get(i);
      orig.set(i, orig.get(j));
      orig.set(j, temp);
    }
  }

  private static LongArrayList reversedCopy(LongArrayList orig) {
    LongArrayList result = new LongArrayList(orig.size());
    for (int i = orig.size() - 1; i >= 0; i--) {
      result.add(orig.get(i));
    }
    return result;
  }

  static LongArrayList appendToSkipFirst(LongArrayList orig, LongArrayList toAppend) {
    int size = orig.size() + toAppend.size() - 1;
    orig.ensureCapacity(size);
    System.arraycopy(toAppend.buffer, 1, orig.buffer, orig.size(), toAppend.size() - 1);
    orig.elementsCount = size;
    return orig;
  }

  static LongArrayList prependToSkipLast(LongArrayList orig, LongArrayList toPrepend) {
    int size = orig.size() + toPrepend.size() - 1;
    orig.ensureCapacity(size);
    System.arraycopy(orig.buffer, 0, orig.buffer, toPrepend.size() - 1, orig.size());
    System.arraycopy(toPrepend.buffer, 0, orig.buffer, 0, toPrepend.size() - 1);
    orig.elementsCount = size;
    return orig;
  }

  static List<LongArrayList> connectPolygonSegments(List<LongArrayList> outer) {
    LongObjectMap<LongArrayList> endpointIndex = new GHLongObjectHashMap<>(outer.size() * 2);
    List<LongArrayList> completeRings = new ArrayList<>(outer.size());

    for (LongArrayList ids : outer) {
      if (ids.size() <= 1) {
        continue;
      }

      long first = ids.get(0);
      long last = ids.get(ids.size() - 1);

      // optimization - skip rings that are already closed (as long as they have enough points)
      if (first == last) {
        if (ids.size() >= 4) {
          completeRings.add(ids);
        }
        continue;
      }

      LongArrayList match, nextMatch;

      if ((match = endpointIndex.get(first)) != null) {
        endpointIndex.remove(first);
        if (first == match.get(0)) {
          reverseInPlace(match);
        }
        appendToSkipFirst(match, ids);
        if ((nextMatch = endpointIndex.get(last)) != null && nextMatch != match) {
          endpointIndex.remove(last);
          if (last == nextMatch.get(0)) {
            appendToSkipFirst(match, nextMatch);
          } else {
            appendToSkipFirst(match, reversedCopy(nextMatch));
          }
          endpointIndex.put(match.get(match.size() - 1), match);
        } else {
          endpointIndex.put(last, match);
        }
      } else if ((match = endpointIndex.get(last)) != null) {
        endpointIndex.remove(last);
        if (last == match.get(0)) {
          prependToSkipLast(match, ids);
        } else {
          appendToSkipFirst(match, reversedCopy(ids));
        }
        endpointIndex.put(first, match);
      } else {
        LongArrayList copy = new LongArrayList(ids);
        endpointIndex.put(first, copy);
        endpointIndex.put(last, copy);
      }
    }

    for (LongObjectCursor<LongArrayList> cursor : endpointIndex) {
      LongArrayList value = cursor.value;
      if (value.size() >= 4) {
        if (value.get(0) == value.get(value.size() - 1) || cursor.key == value.get(0)) {
          completeRings.add(value);
        }
      }
    }
    return completeRings;
  }
}
