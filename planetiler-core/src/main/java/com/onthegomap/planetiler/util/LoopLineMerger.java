package com.onthegomap.planetiler.util;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.PrecisionModel;

public class LoopLineMerger {
  private final List<LineString> input = new ArrayList<>();
  private final List<Node> output = new ArrayList<>();
  int ids = 0;
  private PrecisionModel precisionModel = new PrecisionModel(16);
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
        node.getEdges().clear();
        List<Coordinate> coordinates = new ArrayList<>();
        coordinates.addAll(a.coordinates.reversed());
        coordinates.addAll(b.coordinates.subList(1, b.coordinates.size()));
        Edge c = new Edge(a.to, b.to, coordinates, a.length + b.length);
        a.to.removeEdge(a.reversed);
        b.to.removeEdge(b.reversed);
        a.to.addEdge(c);
        b.to.addEdge(c.reversed);
      }
    }
  }

  private void removeLoops() {
    for (var node : output) {
      for (var edge : List.copyOf(node.getEdges())) {
        var allPaths = findAllPaths(edge.from, edge.to, loopMinLength);
        if (allPaths.size() > 1) {
          for (var path : allPaths.subList(1, allPaths.size())) {
            for (var toRemove : path) {
              toRemove.remove();
            }
          }
        }
      }
    }
  }

  List<List<Edge>> findAllPaths(Node start, Node end, double maxLength) {
    List<List<Edge>> allPaths = new ArrayList<>();
    Queue<List<Edge>> queue = new LinkedList<>();

    for (var edge : start.getEdges()) {
      if (edge.length <= maxLength) {
        queue.add(List.of(edge));
      }
    }

    while (!queue.isEmpty()) {
      List<Edge> currentPath = queue.poll();
      Node currentPoint = currentPath.getLast().to;

      if (currentPoint == end) {
        allPaths.add(new ArrayList<>(currentPath));
      } else {
        for (var edge : currentPoint.getEdges()) {
          if (!currentPath.contains(edge) && !currentPath.contains(edge.reversed)) {
            List<Edge> newPath = new ArrayList<>(currentPath);
            newPath.add(edge);
            if (getLength(newPath) <= maxLength && !hasPointAppearingMoreThanTwice(newPath)) {
              queue.add(newPath);
            }
          }
        }
      }
    }

    allPaths.sort(Comparator.comparingDouble(LoopLineMerger::getLength));

    return allPaths;
  }

  private void removeShortStubEdges() {
    for (var node : output) {
      for (var edge : List.copyOf(node.getEdges())) {
        if (edge.length < minLength && (edge.from.getEdges().size() == 1 || edge.to.getEdges().size() == 1)) {
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
      removeLoops();
      merge();
    }

    if (minLength > 0.0) {
      removeShortStubEdges();
      merge();
      removeShortEdges();
      merge();
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
    int id = ids++;
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
  }

  class Edge {
    Node from;
    Node to;
    double length;
    List<Coordinate> coordinates;
    boolean main;
    Edge reversed;


    private Edge(Node from, Node to, List<Coordinate> coordinateSequence, double length) {
      this(from, to, length, coordinateSequence, true, null);
      reversed = new Edge(to, from, length, coordinateSequence.reversed(), false, this);
    }

    public Edge(Node from, Node to, double length, List<Coordinate> coordinates, boolean main, Edge reversed) {
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

    @Override
    public String toString() {
      return "Edge{" + from.id + "->" + to.id + (main ? "" : "(R)") + ": [" + coordinates.getFirst() + ".." +
        coordinates.getLast() + "], length=" + length + '}';
    }
  }
}
