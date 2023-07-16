package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import com.onthegomap.planetiler.reader.FileFormatException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Files;
import java.nio.file.Path;
import org.locationtech.jts.geom.Geometry;

/**
 * Parse a polygon file used for filtering planetiler output to a specific shape.
 *
 * @see <a href="https://wiki.openstreetmap.org/wiki/Osmosis/Polygon_Filter_File_Format">Osmosis/Polygon Filter File
 *      Format</a>
 */
public class PolyFileReader {

  private PolyFileReader() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Reads a polygon from a file.
   */
  public static Geometry parsePolyFile(Path polyFile) throws IOException {
    try (BufferedReader reader = Files.newBufferedReader(polyFile)) {
      return parsePolyFile(reader);
    }
  }

  /**
   * Reads a polygon from a string.
   */
  public static Geometry parsePolyFile(String polyFile) throws IOException {
    try (Reader reader = new StringReader(polyFile)) {
      return parsePolyFile(reader);
    }
  }

  /**
   * Reads a polygon from a {@link Reader} {@code input}.
   */
  public static Geometry parsePolyFile(Reader input) throws IOException {
    Geometry result = GeoUtils.EMPTY_POLYGON;
    try (BufferedReader reader = input instanceof BufferedReader br ? br : new BufferedReader(input)) {
      String line;
      MutableCoordinateSequence currentRing = null;
      boolean firstLine = true, inRing = false, inPolygon = true, hole = false;
      while ((line = reader.readLine()) != null) {
        if (line.isBlank()) {
          // ingore line
        } else if (!inPolygon) {
          throw new FileFormatException("File continues after end of polygon");
        } else if (firstLine) {
          firstLine = false;
          // first line is junk.
        } else if (inRing) {
          if (line.strip().equals("END")) {
            // we are at the end of a ring, perhaps with more to come.
            currentRing.closeRing();
            var polygon = JTS_FACTORY.createPolygon(JTS_FACTORY.createLinearRing(currentRing), null);
            if (hole) {
              result = result.difference(polygon);
            } else {
              result = result.union(polygon);
            }
            currentRing = null;
            inRing = false;
          } else {
            // we are in a ring and picking up new coordinates.
            String[] splitted = line.trim().split("\\s+");
            currentRing.addPoint(Double.parseDouble(splitted[0]), Double.parseDouble(splitted[1]));
          }
        } else {
          if (line.strip().equals("END")) {
            // we are at the end of the whole polygon.
            inPolygon = false;
          } else {
            // we are at the start of a polygon part.
            currentRing = new MutableCoordinateSequence();
            hole = line.strip().charAt(0) == '!';
            inRing = true;
          }
        }
      }

      if (inRing) {
        throw new FileFormatException("Unclosed ring");
      }
      if (inPolygon) {
        throw new FileFormatException("File ends before end of polygon");
      }

      return result;
    }
  }
}
