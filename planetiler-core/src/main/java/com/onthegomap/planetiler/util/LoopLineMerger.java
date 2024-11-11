package com.onthegomap.planetiler.util;

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


public class LoopLineMerger {
  final List<LineString> input = new ArrayList<>();
  private final List<Node> output = new ArrayList<>();
  int nodes = 0;
  int edges = 0;
  private PrecisionModel precisionModel = new PrecisionModel(GeoUtils.TILE_PRECISION);
  private GeometryFactory factory = new GeometryFactory(precisionModel);
  private double minLength = 0.0;
  private double loopMinLength = 0.0;

  private static double getLength(List<Edge> lines) {
    double result = 0.0;
    for (var line : lines) {
      result += line.length;
    }
    return result;
  }

  static boolean hasPointAppearingMoreThanTwice(List<Edge> edges) {
    HashMap<Node, Integer> pointCountMap = new HashMap<>();
    for (Edge edge : edges) {
      if (pointCountMap.merge(edge.from, 1, Integer::sum) > 2) {
        return true;
      }
      if (pointCountMap.merge(edge.to, 1, Integer::sum) > 2) {
        return true;
      }
    }
    return false;
  }

  public LoopLineMerger setPrecisionModel(PrecisionModel precisionModel) {
    this.precisionModel = precisionModel;
    factory = new GeometryFactory(precisionModel);
    return this;
  }

  public LoopLineMerger setMinLength(double minLength) {
    this.minLength = minLength;
    return this;
  }

  public LoopLineMerger setLoopMinLength(double loopMinLength) {
    this.loopMinLength = loopMinLength;
    return this;
  }

  public void add(Geometry geometry) {
    geometry.apply((GeometryComponentFilter) component -> {
      if (component instanceof LineString lineString) {
        input.add(lineString);
      }
    });
  }

  private void merge() {
    for (var node : output) {
      if (node.getEdges().size() == 2) {
        Edge a = node.getEdges().getFirst();
        Edge b = node.getEdges().get(1);
        mergeTwoEdges(a, b);
      }
    }
  }

  private void mergeTwoEdges(Edge a, Edge b) {
    assert a.to == b.from;
    a.to.getEdges().remove(a);
    b.from.getEdges().remove(b);
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
  }

  private void mergeByAngle() {
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
          mergeTwoEdges(angledPair.a, angledPair.b);
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
            shortestDistance(other.to, current.to, current.from, loopMinLength - other.length);
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

  private double shortestDistance(Node start, Node end, Node exclude, double maxLength) {
    Map<Integer, Double> bestDistance = new HashMap<>();
    record Candidate(Node node, double cost, double heuristic) {}
    PriorityQueue<Candidate> frontier = new PriorityQueue<>(Comparator.comparingDouble(Candidate::heuristic));
    if (exclude != start) {
      frontier.offer(new Candidate(start, 0, start.distance(end)));
    }
    while (!frontier.isEmpty()) {
      Candidate candidate = frontier.poll();
      Node current = candidate.node;
      if (current == end) {
        return candidate.cost;
      }

      for (var edge : current.getEdges()) {
        var neighbor = edge.to;
        if (neighbor != exclude) {
          double newDist = candidate.cost + edge.length;
          double prev = bestDistance.getOrDefault(neighbor.id, Double.POSITIVE_INFINITY);
          if (newDist < prev) {
            bestDistance.put(neighbor.id, newDist);
            double heuristic = newDist + neighbor.distance(end);
            if (heuristic <= maxLength) {
              frontier.offer(new Candidate(neighbor, newDist, heuristic));
            }
          }
        }
      }
    }
    return Double.POSITIVE_INFINITY;
  }

  private void removeShortStubEdges(double stubMinLength) {
    for (var node : output) {
      for (var edge : List.copyOf(node.getEdges())) {
        if (edge.length < stubMinLength &&
          (edge.from.getEdges().size() == 1 || edge.to.getEdges().size() == 1 || edge.from == edge.to)) {
          edge.remove();
        }
      }
    }
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

  public List<LineString> getMergedLineStrings() {
    List<List<Coordinate>> edges = nodeLines(input);
    buildNodes(edges);

    merge();

    if (loopMinLength > 0.0) {
      breakLoops();
      merge();
    }

    if (minLength > 0.0) {
      double step = 1.0 / precisionModel.getScale();
      for (double stubMinLength = 0.0; stubMinLength < minLength; stubMinLength += step) {
        removeShortStubEdges(stubMinLength);
        merge();
      }
      removeShortStubEdges(minLength);
      merge();
      // minLength = 10 * 0.0625;
      // removeShortEdges();
      // merge();
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

  public List<LineString> getMergedByAngle() {
    List<List<Coordinate>> edges = nodeLines(input);
    buildNodes(edges);

    mergeByAngle();

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

  private double length(List<Coordinate> edge) {
    Coordinate last = null;
    double length = 0;
    // we only care about the length of lines if they are < the limit
    // so stop counting once we exceed it
    double maxLengthToTrack = Math.max(minLength, loopMinLength);
    if (maxLengthToTrack <= 0) {
      return Double.POSITIVE_INFINITY;
    }
    for (Coordinate coord : edge) {
      if (last != null) {
        length += last.distance(coord);
        if (length > maxLengthToTrack) {
          return Double.POSITIVE_INFINITY;
        }
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
        firstNode = new Node();
        nodes.put(first, firstNode);
        output.add(firstNode);
      }

      Coordinate last = coordinateSequence.getLast();
      Node lastNode = nodes.get(last);
      if (lastNode == null) {
        lastNode = new Node();
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

  class Node {
    int id = nodes++;
    List<Edge> edge = new ArrayList<>();

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

    public double distance(Node end) {
      if (!getEdges().isEmpty() && !end.getEdges().isEmpty()) {
        Coordinate a = getEdges().getFirst().coordinates.getFirst();
        Coordinate b = end.getEdges().getFirst().coordinates.getFirst();
        return a.distance(b);
      }
      return Double.POSITIVE_INFINITY;
    }
  }

  class Edge {

    public int id;
    Node from;
    Node to;
    double length;
    List<Coordinate> coordinates;
    boolean main;
    Edge reversed;


    private Edge(Node from, Node to, List<Coordinate> coordinateSequence, double length) {
      this(edges, from, to, length, coordinateSequence, true, null);
      reversed = new Edge(edges, to, from, length, coordinateSequence.reversed(), false, this);
      edges++;
    }

    public Edge(int id, Node from, Node to, double length, List<Coordinate> coordinates, boolean main, Edge reversed) {
      this.id = id;
      this.from = from;
      this.to = to;
      this.length = length;
      this.coordinates = coordinates;
      this.main = main;
      this.reversed = reversed;
    }

    public void remove() {
      from.removeEdge(this);
      to.removeEdge(reversed);
    }

    double angleTo(Edge other) {
      assert from.equals(other.from); 
      assert coordinates.size() >= 2;

      double angle = Angle.angle(coordinates.get(0), coordinates.get(1));
      double angleOther = Angle.angle(other.coordinates.get(0), other.coordinates.get(1));
      
      return Math.abs(Angle.normalize(angle - angleOther));
    }

    @Override
    public String toString() {
      return "Edge{" + from.id + "->" + to.id + (main ? "" : "(R)") + ": [" + coordinates.getFirst() + ".." +
        coordinates.getLast() + "], length=" + length + '}';
    }
  }
}
