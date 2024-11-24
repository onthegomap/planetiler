package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.DouglasPeuckerSimplifier;
import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import org.locationtech.jts.algorithm.Angle;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.operation.linemerge.LineMerger;

/**
 * A utility class for merging, simplifying, and connecting linestrings and removing small loops.
 * <p>
 * Compared to JTS {@link LineMerger} which only connects when 2 lines meet at a single point, this utility:
 * <ul>
 * <li>snap-rounds points to a grid
 * <li>splits lines that intersect at a midpoint
 * <li>breaks small loops less than {@code loopMinLength} so only the shortest path connects both endpoints of the loop
 * <li>removes short "hair" edges less than {@code stubMinLength} coming off the side of longer segments
 * <li>simplifies linestrings, without touching the points shared between multiple lines to avoid breaking connections
 * <li>removes any duplicate edges
 * <li>at any remaining 3+ way intersections, connect pairs of edges that form the straightest path through the node
 * <li>remove any remaining edges shorter than {@code minLength}
 * </ul>
 *
 * @see <a href= "https://oliverwipfli.ch/improving-linestring-merging-in-planetiler-2024-10-30/">Improving Linestring
 *      Merging in Planetiler</a>
 */
public class LoopLineMerger {
  private final List<LineString> input = new ArrayList<>();
  private final List<Node> output = new ArrayList<>();
  private int nodes = 0;
  private int edges = 0;
  private PrecisionModel precisionModel = new PrecisionModel(GeoUtils.TILE_PRECISION);
  private GeometryFactory factory = new GeometryFactory(precisionModel);
  private double minLength = 0.0;
  private double loopMinLength = 0.0;
  private double stubMinLength = 0.0;
  private double tolerance = -1.0;
  private boolean mergeStrokes = false;

  /**
   * Sets the precision model used to snap points to a grid.
   * <p>
   * Use {@link PrecisionModel#FLOATING} to not snap points at all, or {@code new PrecisionModel(4)} to snap to a 0.25px
   * grid.
   */
  public LoopLineMerger setPrecisionModel(PrecisionModel precisionModel) {
    this.precisionModel = precisionModel;
    factory = new GeometryFactory(precisionModel);
    return this;
  }

  /**
   * Sets the minimum length for retaining linestrings in the resulting geometry.
   * <p>
   * Linestrings shorter than this value will be removed. {@code minLength <= 0} disables min length removal.
   */
  public LoopLineMerger setMinLength(double minLength) {
    this.minLength = minLength;
    return this;
  }

  /**
   * Sets the minimum loop length for breaking loops in the merged geometry.
   * <p>
   * Loops that are shorter than loopMinLength are broken up so that only the shortest path between loop endpoints
   * remains. This should be {@code >= minLength}. {@code loopMinLength <= 0} disables loop removal.
   */
  public LoopLineMerger setLoopMinLength(double loopMinLength) {
    this.loopMinLength = loopMinLength;
    return this;
  }

  /**
   * Sets the minimum length of stubs to be removed during processing.
   * <p>
   * Stubs are short "hair" line segments that hang off of a longer linestring without connecting to anything else.
   * {@code stubMinLength <= 0} disables stub removal.
   */
  public LoopLineMerger setStubMinLength(double stubMinLength) {
    this.stubMinLength = stubMinLength;
    return this;
  }

  /**
   * Sets the tolerance for simplifying linestrings during processing. Lines are simplified between endpoints to avoid
   * breaking intersections.
   * <p>
   * {@code tolerance = 0} still removes collinear points, so you need to set {@code tolerance <= 0} to disable
   * simplification.
   */
  public LoopLineMerger setTolerance(double tolerance) {
    this.tolerance = tolerance;
    return this;
  }

  /**
   * Enables or disables stroke merging. Stroke merging connects the straightest pairs of linestrings at junctions with
   * 3 or more attached linestrings based on the angle between them.
   */
  public LoopLineMerger setMergeStrokes(boolean mergeStrokes) {
    this.mergeStrokes = mergeStrokes;
    return this;
  }

  /**
   * Adds a geometry to the merger. Only linestrings from the input geometry are considered.
   */
  public LoopLineMerger add(Geometry geometry) {
    geometry.apply((GeometryComponentFilter) component -> {
      if (component instanceof LineString lineString) {
        input.add(lineString);
      }
    });
    return this;
  }

  private void degreeTwoMerge() {
    for (var node : output) {
      degreeTwoMerge(node);
    }
  }

  private Edge degreeTwoMerge(Node node) {
    if (node.getEdges().size() == 2) {
      Edge a = node.getEdges().getFirst();
      Edge b = node.getEdges().get(1);
      return mergeTwoEdges(node, a, b);
    }
    return null;
  }

  private Edge mergeTwoEdges(Node node, Edge edge1, Edge edge2) {
    // attempt to preserve segment directions from the original line
    // when: A << N -- B then output C reversed from B to A
    // when: A >> N -- B then output C from A to B
    Edge a = edge1.main ? edge2 : edge1;
    Edge b = edge1.main ? edge1 : edge2;
    node.getEdges().remove(a);
    node.getEdges().remove(b);
    List<Coordinate> coordinates = new ArrayList<>();
    coordinates.addAll(a.coordinates.reversed());
    coordinates.addAll(b.coordinates.subList(1, b.coordinates.size()));
    Edge c = new Edge(a.to, b.to, coordinates, a.length + b.length);
    a.to.removeEdge(a.reversed);
    b.to.removeEdge(b.reversed);
    a.to.addEdge(c);
    if (a.to != b.to) {
      b.to.addEdge(c.reversed);
    }
    return c;
  }

  private void strokeMerge() {
    for (var node : output) {
      List<Edge> edges = List.copyOf(node.getEdges());
      if (edges.size() >= 3) {
        record AngledPair(Edge a, Edge b, double angle) {}
        List<AngledPair> angledPairs = new ArrayList<>();
        for (var i = 0; i < edges.size(); ++i) {
          for (var j = i + 1; j < edges.size(); ++j) {
            double angle = edges.get(i).angleTo(edges.get(j));
            angledPairs.add(new AngledPair(edges.get(i), edges.get(j), angle));
          }
        }
        angledPairs.sort(Comparator.comparingDouble(angledPair -> angledPair.angle));
        List<Edge> merged = new ArrayList<>();
        for (var angledPair : angledPairs.reversed()) {
          if (merged.contains(angledPair.a) || merged.contains(angledPair.b)) {
            continue;
          }
          mergeTwoEdges(angledPair.a.from, angledPair.a, angledPair.b);
          merged.add(angledPair.a);
          merged.add(angledPair.b);
        }
      }
    }
  }

  private void breakLoops() {
    for (var node : output) {
      if (node.getEdges().size() <= 1) {
        continue;
      }
      for (var current : List.copyOf(node.getEdges())) {
        record HasLoop(Edge edge, double distance) {}
        List<HasLoop> loops = new ArrayList<>();
        if (!node.getEdges().contains(current)) {
          continue;
        }
        for (var other : node.getEdges()) {
          double distance = other.length +
            shortestDistanceAStar(other.to, current.to, current.from, loopMinLength - other.length);
          if (distance <= loopMinLength) {
            loops.add(new HasLoop(other, distance));
          }
        }
        if (loops.size() > 1) {
          HasLoop min = loops.stream().min(Comparator.comparingDouble(HasLoop::distance)).get();
          for (var loop : loops) {
            if (loop != min) {
              loop.edge.remove();
            }
          }
        }
      }
    }
  }

  private double shortestDistanceAStar(Node start, Node end, Node exclude, double maxLength) {
    Map<Integer, Double> bestDistance = new HashMap<>();
    record Candidate(Node node, double length, double minTotalLength) {}
    PriorityQueue<Candidate> frontier = new PriorityQueue<>(Comparator.comparingDouble(Candidate::minTotalLength));
    if (exclude != start) {
      frontier.offer(new Candidate(start, 0, start.distance(end)));
    }
    while (!frontier.isEmpty()) {
      Candidate candidate = frontier.poll();
      Node current = candidate.node;
      if (current == end) {
        return candidate.length;
      }

      for (var edge : current.getEdges()) {
        var neighbor = edge.to;
        if (neighbor != exclude) {
          double newDist = candidate.length + edge.length;
          double prev = bestDistance.getOrDefault(neighbor.id, Double.POSITIVE_INFINITY);
          if (newDist < prev) {
            bestDistance.put(neighbor.id, newDist);
            double minTotalLength = newDist + neighbor.distance(end);
            if (minTotalLength <= maxLength) {
              frontier.offer(new Candidate(neighbor, newDist, minTotalLength));
            }
          }
        }
      }
    }
    return Double.POSITIVE_INFINITY;
  }

  private void removeShortStubEdges() {
    PriorityQueue<Edge> toRemove = new PriorityQueue<>(Comparator.comparingDouble(Edge::length));
    for (var node : output) {
      for (var edge : node.getEdges()) {
        if (isShortStubEdge(edge)) {
          toRemove.offer(edge);
        }
      }
    }
    while (!toRemove.isEmpty()) {
      var edge = toRemove.poll();
      if (edge.removed) {
        continue;
      }
      edge.remove();
      if (degreeTwoMerge(edge.from) instanceof Edge merged && isShortStubEdge(merged)) {
        toRemove.offer(merged);
      }
      if (edge.from.getEdges().size() == 1) {
        var other = edge.from.getEdges().getFirst();
        if (isShortStubEdge(other)) {
          toRemove.offer(other);
        }
      }
      if (edge.from != edge.to) {
        if (degreeTwoMerge(edge.to) instanceof Edge merged && isShortStubEdge(merged)) {
          toRemove.offer(merged);
        }
        if (edge.to.getEdges().size() == 1) {
          var other = edge.to.getEdges().getFirst();
          if (isShortStubEdge(other)) {
            toRemove.offer(other);
          }
        }
      }
    }
  }

  private boolean isShortStubEdge(Edge edge) {
    return edge != null && !edge.removed && edge.length < stubMinLength &&
      (edge.from.getEdges().size() == 1 || edge.to.getEdges().size() == 1 || edge.from == edge.to);
  }

  private void removeShortEdges() {
    for (var node : output) {
      for (var edge : List.copyOf(node.getEdges())) {
        if (edge.length < minLength) {
          edge.remove();
        }
      }
    }
  }

  private void simplify() {
    List<Edge> toRemove = new ArrayList<>();
    for (var node : output) {
      for (var edge : node.getEdges()) {
        if (edge.main) {
          if (!edge.simplify()) {
            toRemove.add(edge);
          }
        }
      }
    }
    toRemove.forEach(Edge::remove);
  }

  private void removeDuplicatedEdges() {
    for (var node : output) {
      List<Edge> toRemove = new ArrayList<>();
      for (var i = 0; i < node.getEdges().size(); ++i) {
        Edge a = node.getEdges().get(i);
        for (var j = i + 1; j < node.getEdges().size(); ++j) {
          Edge b = node.getEdges().get(j);
          if (b.to == a.to && a.coordinates.equals(b.coordinates)) {
            toRemove.add(b);
          }
        }
      }
      for (var edge : toRemove) {
        edge.remove();
      }
    }
  }

  /**
   * Processes the added geometries and returns the merged linestrings.
   * <p>
   * Can be called more than once.
   */
  public List<LineString> getMergedLineStrings() {
    output.clear();
    List<List<Coordinate>> edges = nodeLines(input);
    buildNodes(edges);

    degreeTwoMerge();

    if (loopMinLength > 0.0) {
      breakLoops();
      degreeTwoMerge();
    }

    if (stubMinLength > 0.0) {
      removeShortStubEdges();
      // removeShortStubEdges does degreeTwoMerge internally
    }

    if (tolerance >= 0.0) {
      simplify();
      removeDuplicatedEdges();
      degreeTwoMerge();
    }

    if (mergeStrokes) {
      strokeMerge();
      degreeTwoMerge();
    }

    if (minLength > 0) {
      removeShortEdges();
    }

    List<LineString> result = new ArrayList<>();

    for (var node : output) {
      for (var edge : node.getEdges()) {
        if (edge.main) {
          result.add(factory.createLineString(edge.coordinates.toArray(Coordinate[]::new)));
        }
      }
    }

    return result;
  }

  private static double length(List<Coordinate> edge) {
    Coordinate last = null;
    double length = 0;
    for (Coordinate coord : edge) {
      if (last != null) {
        length += last.distance(coord);
      }
      last = coord;
    }
    return length;
  }

  private void buildNodes(List<List<Coordinate>> edges) {
    Map<Coordinate, Node> nodes = new HashMap<>();
    for (var coordinateSequence : edges) {
      Coordinate first = coordinateSequence.getFirst();
      Node firstNode = nodes.get(first);
      if (firstNode == null) {
        firstNode = new Node(first);
        nodes.put(first, firstNode);
        output.add(firstNode);
      }

      Coordinate last = coordinateSequence.getLast();
      Node lastNode = nodes.get(last);
      if (lastNode == null) {
        lastNode = new Node(last);
        nodes.put(last, lastNode);
        output.add(lastNode);
      }

      double length = length(coordinateSequence);

      Edge edge = new Edge(firstNode, lastNode, coordinateSequence, length);

      firstNode.addEdge(edge);
      lastNode.addEdge(edge.reversed);
    }
  }

  private List<List<Coordinate>> nodeLines(List<LineString> input) {
    Map<Coordinate, Integer> nodeCounts = new HashMap<>();
    List<List<Coordinate>> coords = new ArrayList<>(input.size());
    for (var line : input) {
      var coordinateSequence = line.getCoordinateSequence();
      List<Coordinate> snapped = new ArrayList<>();
      Coordinate last = null;
      for (int i = 0; i < coordinateSequence.size(); i++) {
        Coordinate current = new CoordinateXY(coordinateSequence.getX(i), coordinateSequence.getY(i));
        precisionModel.makePrecise(current);
        if (last == null || !last.equals(current)) {
          snapped.add(current);
          nodeCounts.merge(current, 1, Integer::sum);
        }
        last = current;
      }
      if (snapped.size() >= 2) {
        coords.add(snapped);
      }
    }

    List<List<Coordinate>> result = new ArrayList<>(input.size());
    for (var coordinateSequence : coords) {
      int start = 0;
      for (int i = 0; i < coordinateSequence.size(); i++) {
        Coordinate coordinate = coordinateSequence.get(i);
        if (i > 0 && i < coordinateSequence.size() - 1 && nodeCounts.get(coordinate) > 1) {
          result.add(coordinateSequence.subList(start, i + 1));
          start = i;
        }
      }
      if (start < coordinateSequence.size()) {
        var sublist = start == 0 ? coordinateSequence : coordinateSequence.subList(start, coordinateSequence.size());
        result.add(sublist);
      }
    }
    return result;
  }

  private class Node {
    final int id = nodes++;
    final List<Edge> edge = new ArrayList<>();
    Coordinate coordinate;

    Node(Coordinate coordinate) {
      this.coordinate = coordinate;
    }

    void addEdge(Edge edge) {
      for (Edge other : this.edge) {
        if (other.coordinates.equals(edge.coordinates)) {
          return;
        }
      }
      this.edge.add(edge);
    }

    List<Edge> getEdges() {
      return edge;
    }

    void removeEdge(Edge edge) {
      this.edge.remove(edge);
    }

    @Override
    public String toString() {
      return "Node{" + id + ": " + edge + '}';
    }

    double distance(Node end) {
      return coordinate.distance(end.coordinate);
    }
  }

  private class Edge {

    final int id;
    final Node from;
    final Node to;
    final double length;
    final boolean main;
    boolean removed;

    Edge reversed;
    List<Coordinate> coordinates;


    private Edge(Node from, Node to, List<Coordinate> coordinateSequence, double length) {
      this(edges, from, to, length, coordinateSequence, true, null);
      reversed = new Edge(edges, to, from, length, coordinateSequence.reversed(), false, this);
      edges++;
    }

    private Edge(int id, Node from, Node to, double length, List<Coordinate> coordinates, boolean main, Edge reversed) {
      this.id = id;
      this.from = from;
      this.to = to;
      this.length = length;
      this.coordinates = coordinates;
      this.main = main;
      this.reversed = reversed;
    }

    void remove() {
      if (!removed) {
        from.removeEdge(this);
        to.removeEdge(reversed);
        removed = true;
      }
    }

    double angleTo(Edge other) {
      assert from.equals(other.from);
      assert coordinates.size() >= 2;

      double angle = Angle.angle(coordinates.get(0), coordinates.get(1));
      double angleOther = Angle.angle(other.coordinates.get(0), other.coordinates.get(1));

      return Math.abs(Angle.normalize(angle - angleOther));
    }

    double length() {
      return length;
    }

    /** Returns {@code false} if simplifying this edge collapsed it to {@code length=0}. */
    boolean simplify() {
      coordinates = DouglasPeuckerSimplifier.simplify(coordinates, tolerance, false);
      if (reversed != null) {
        reversed.coordinates = coordinates.reversed();
      }
      return coordinates.size() != 2 || !coordinates.getFirst().equals(coordinates.getLast());
    }

    @Override
    public String toString() {
      return "Edge{" + from.id + "->" + to.id + (main ? "" : "(R)") + ": [" + coordinates.getFirst() + ".." +
        coordinates.getLast() + "], length=" + length + '}';
    }
  }
}
