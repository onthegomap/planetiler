package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolyFileReader {
  private static final Logger LOGGER = LoggerFactory.getLogger(PolyFileReader.class);

  private PolyFileReader() {
    throw new IllegalStateException("Utility class");
  }

  public static MultiPolygon parsePolyFile(Path filePath) throws NumberFormatException, IOException {
    if (filePath == null) {
      return null;
    }

    try (
      BufferedReader br = Files.newBufferedReader(filePath);
    ) {
      boolean inRing = false;
      String line;
      List<Polygon> polygons = new ArrayList<>();
      MutableCoordinateSequence currentRing = null;
      boolean firstLine = true;
      while ((line = br.readLine()) != null) {
        if (firstLine) {
          firstLine = false;
          // first line is junk.
        } else if (inRing) {
          if (line.strip().equals("END")) {
            // we are at the end of a ring, perhaps with more to come.
            if (currentRing != null) {
              currentRing.closeRing();
              polygons.add(JTS_FACTORY
                .createPolygon(
                  JTS_FACTORY
                    .createLinearRing(currentRing),
                  null));
              currentRing = null;
            }
            inRing = false;
          } else if (currentRing != null) {
            // we are in a ring and picking up new coordinates.
            String[] splitted = line.trim().split("\s+");
            currentRing.addPoint(Float.parseFloat(splitted[0]), Float.parseFloat(splitted[1]));
          }
        } else {
          if (line.strip().equals("END")) {
            // we are at the end of the whole polygon.
            break;
          } else if (line.charAt(0) == '!') {
            // we ignore holes for now
            inRing = true;
          } else {
            // we are at the start of a polygon part.
            currentRing = new MutableCoordinateSequence();
            inRing = true;
          }
        }
      }

      return JTS_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
    } catch (Exception e) {
      LOGGER.error("Failed to parse poly file {} : {}", filePath, e);
      return null;
    }
  }
}
