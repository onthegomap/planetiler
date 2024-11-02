package com.onthegomap.planetiler.util;

import java.util.ArrayList;
import java.util.List;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryComponentFilter;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;

public class LoopLineMerger {
  private HashMap<Point, Node> graph = new HashMap<>();
  private GeometryFactory factory = null;


  public LoopLineMerger() {
  }

  public void add(Geometry geometry) {
    geometry.apply(new GeometryComponentFilter() {
      public void filter(Geometry component) {
        if (component instanceof LineString) {
          LineString lineString = (LineString) component;
          if (factory == null) {
            factory = lineString.getFactory();
          }
          var segments = split(lineString);
          for (var segment : segments) {
            add(segment);
          }
        }
      }
    });
  }

  private void add(LineString lineString) {
    var A = lineString.getStartPoint();
    if (graph.containsKey(A)) {
      var node = graph.get(A);
      node.addEdge(lineString);
    } else {
      var node = new Node(A);
      node.addEdge(lineString);
      graph.put(A, node);
    }

    var B = lineString.getEndPoint();
    if (graph.containsKey(B)) {
      var node = graph.get(B);
      node.addEdge(lineString);
    } else {
      var node = new Node(B);
      node.addEdge(lineString);
      graph.put(B, node);
    }

  }

  private Coordinate roundCoordinate(Coordinate cooridnate) {
    Coordinate result = new Coordinate(cooridnate);

    double multiplier = 16.0;

    result.x = Math.round(result.x * multiplier) / multiplier;
    result.y = Math.round(result.y * multiplier) / multiplier;

    return result;
  }

  private List<LineString> split(LineString lineString) {
    List<LineString> segments = new ArrayList<>();

    Coordinate[] coordinates = lineString.getCoordinates();
    for (var i = 0; i < coordinates.length - 1; ++i) {
      Coordinate[] segmentCoordinates = { roundCoordinate(coordinates[i]), roundCoordinate(coordinates[i + 1]) };
      LineString segment = factory.createLineString(segmentCoordinates);
      if (segment.getLength() > 0.0) {
        segments.add(segment);
      }
    }

    return segments;
  }

  public LineString concat(LineString A, LineString B) {

    List<Coordinate> coordinates = new ArrayList<>();
    List<Coordinate> coordsA = List.of(A.getCoordinates());
    List<Coordinate> coordsB = List.of(B.getCoordinates());

    if (A.getEndPoint().equals(B.getStartPoint())) {
      coordinates.addAll(coordsA);
      coordinates.addAll(coordsB.subList(1, coordsB.size()));
    } else if (B.getEndPoint().equals(A.getStartPoint())) {
      coordinates.addAll(coordsB);
      coordinates.addAll(coordsA.subList(1, coordsA.size()));
    } else if (A.getStartPoint().equals(B.getStartPoint())) {
      coordinates.addAll(coordsA.reversed());
      coordinates.addAll(coordsB.subList(1, coordsB.size()));
    } else if (A.getEndPoint().equals(B.getEndPoint())) {
      coordinates.addAll(coordsA);
      coordinates.addAll(coordsB.reversed().subList(1, coordsB.size()));
    } else {
      System.out.println("ERROR in concat().");
    }
    return factory.createLineString(coordinates.toArray(new Coordinate[0]));
  }

  private void merge() {
    for (var point : graph.keySet()) {
      var node = graph.get(point);
      if (node.getEdges().size() == 2) {

        var A = node.getEdges().get(0);
        var B = node.getEdges().get(1);

        graph.get(A.getStartPoint()).removeEdge(A);
        graph.get(A.getEndPoint()).removeEdge(A);

        graph.get(B.getStartPoint()).removeEdge(B);
        graph.get(B.getEndPoint()).removeEdge(B);

        var C = concat(A, B);

        graph.get(C.getStartPoint()).addEdge((LineString) C.copy());
        graph.get(C.getEndPoint()).addEdge((LineString) C.copy());
      }
    }

    // remove nodes that do not have edges attached anymore
    graph.entrySet().removeIf(entry -> entry.getValue().getEdges().size() == 0);
  }

  private void removeLoops(double loopMinLength) {

    var points = new ArrayList<>(graph.keySet());
    for (var currentPoint : points) {

      var node = graph.get(currentPoint);
      if (node == null) {
        continue;
      }

      var edges = new ArrayList<>(node.getEdges());
      while (edges.size() > 0) {
        var currentEdge = edges.get(0);
        edges.remove(0);

        if (!node.getEdges().contains(currentEdge)) {
          continue;
        }

        var A = currentEdge.getStartPoint();
        var B = currentEdge.getEndPoint();

        var end = currentPoint.equals(A) ? B : A;
        var allPaths = findAllPaths(currentPoint, end, loopMinLength);

        if (allPaths.size() > 1) {
          for (var path : allPaths.subList(1, allPaths.size())) {
            if (path.size() > 0) {
              var firstEdge = path.get(0);
              var startNode = graph.get(firstEdge.getStartPoint());
              if (startNode != null) {
                startNode.removeEdge(firstEdge);
              }
              var endNode = graph.get(firstEdge.getEndPoint());
              if (endNode != null) {
                endNode.removeEdge(firstEdge);
              }
            }
          }
        }
      }
    }
  }

  private List<List<LineString>> findAllPaths(Point start, Point end, double maxLength) {
    List<List<LineString>> allPaths = new ArrayList<>();
    Queue<List<LineString>> queue = new LinkedList<>();

    Node node = graph.get(start);
    for (var edge : node.getEdges()) {
      var forward = start.equals(edge.getStartPoint());
      if (!forward) {
        edge = edge.reverse();
      }
      List<LineString> path = new ArrayList<>();
      path.add(edge);
      queue.add(path);
    }

    while (!queue.isEmpty()) {
      List<LineString> currentPath = queue.poll();
      Point currentPoint = currentPath.get(currentPath.size() - 1).getEndPoint();

      if (currentPoint.equals(end)) {
        allPaths.add(new ArrayList<>(currentPath));
      } else {
        node = graph.get(currentPoint);
        if (node != null) {
          for (var edge : node.getEdges()) {

            if (currentPath.contains(edge) || currentPath.contains(edge.reverse())) {
              continue;
            }

            var forward = currentPoint.equals(edge.getStartPoint());
            if (!forward) {
              edge = edge.reverse();
            }

            List<LineString> newPath = new ArrayList<>(currentPath);
            newPath.add(edge);
            if (maxLength > 0.0) {
              if (getLength(newPath) <= maxLength) {
                queue.add(newPath);
              }
            } else {
              queue.add(newPath);
            }
          }
        }
      }
    }

    allPaths.removeIf(a -> hasPointAppearingMoreThanTwice(a));

    allPaths.sort((a, b) -> Double.compare(getLength(a), getLength(b)));

    return allPaths;
  }

  private static double getLength(List<LineString> lines) {
    double result = 0.0;
    for (var line : lines) {
      result += line.getLength();
    }
    return result;
  }

  public static boolean hasPointAppearingMoreThanTwice(List<LineString> lineStrings) {
    HashMap<Point, Integer> pointCountMap = new HashMap<>();
    for (LineString line : lineStrings) {
      Point startPoint = line.getStartPoint();
      Point endPoint = line.getEndPoint();
      pointCountMap.put(startPoint, pointCountMap.getOrDefault(startPoint, 0) + 1);
      pointCountMap.put(endPoint, pointCountMap.getOrDefault(endPoint, 0) + 1);
    }
    for (int count : pointCountMap.values()) {
      if (count > 2) {
        return true;
      }
    }
    return false;
  }

  private void removeShortStubEdges(double minLength) {
    for (var node : graph.values()) {
      for (var edge : new ArrayList<>(node.getEdges())) {
        if (edge.getLength() < minLength) {
          boolean remove = false;
          if (graph.get(edge.getStartPoint()).getEdges().size() == 1) {
            remove = true;
          }
          if (graph.get(edge.getEndPoint()).getEdges().size() == 1) {
            remove = true;
          }
          if (remove) {
            graph.get(edge.getStartPoint()).removeEdge(edge);
            graph.get(edge.getEndPoint()).removeEdge(edge);
          }
        }
      }
    }
  }

  private void removeShortEdges(double minLength) {
    for (var node : graph.values()) {
      for (var edge : new ArrayList<>(node.getEdges())) {
        if (edge.getLength() < minLength) {
          graph.get(edge.getStartPoint()).removeEdge(edge);
          graph.get(edge.getEndPoint()).removeEdge(edge);
        }
      }
    }
  }

  public List<LineString> getMergedLineStrings(double minLength, double loopMinLength) {
    merge();

    if (loopMinLength > 0.0) {
      removeLoops(loopMinLength);
      merge();
    }

    if (minLength > 0.0) {
      removeShortStubEdges(minLength);
      merge();
      removeShortEdges(minLength);
      merge();
    }

    List<LineString> result = new ArrayList<>();

    for (var node : graph.values()) {
      for (var edge : node.getEdges()) {
        if (result.contains(edge) || result.contains(edge.reverse())) {
          continue;
        }
        result.add(edge);
      }
    }

    return result;
  }
}

class Node {
  private Point point;
  private List<LineString> edges;

  public Node(Point point) {
    this.point = point;
    this.edges = new ArrayList<>();
  }

  public Point getPoint() {
    return point;
  }

  public void setPoint(Point point) {
    this.point = point;
  }

  public List<LineString> getEdges() {
    return edges;
  }

  public void addEdge(LineString edge) {
    if (!edges.contains(edge) && !edges.contains(edge.reverse())) {
      edges.add(edge);
    }
  }

  public void removeEdge(LineString edge) {
    if (edges.contains(edge)) {
      edges.remove(edge);
    } else if (edges.contains(edge.reverse())) {
      edges.remove(edge.reverse());
    } else {
      // nothing to do
    }
  }

  @Override
  public String toString() {
    return "Node{" +
        "point=" + point +
        ", edges=" + edges +
        '}';
  }
}
