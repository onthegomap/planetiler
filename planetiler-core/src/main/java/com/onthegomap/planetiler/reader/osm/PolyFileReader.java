package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PolyFileReader {
    private static final Logger LOGGER = LoggerFactory.getLogger(PolyFileReader.class);
    public static MultiPolygon parsePolyFile(String filePath) throws NumberFormatException, IOException {
        if (filePath == null) {
            return null;
        }

        boolean in_ring = false;
        List<Polygon> polygons = new ArrayList<Polygon>();
        List<Coordinate> currentRing = null;
        File file = new File(filePath);
        FileReader fr = new FileReader(file);
        BufferedReader br = new BufferedReader(fr);
        String line;
        int index = 0;
        while ((line = br.readLine()) != null) {
            if (index++ == 0) {
                // first line is junk.
                continue;
            } else if (in_ring && line.strip().equals("END")) {
                if (currentRing != null) {
                    polygons.add(JTS_FACTORY
                        .createPolygon(JTS_FACTORY.createLinearRing(currentRing.toArray(new Coordinate[currentRing.size()])), null));
                    currentRing = null;
                }
                // we are at the end of a ring, perhaps with more to come.
                in_ring = false;
            }

            else if (in_ring) {
                // we are in a ring and picking up new coordinates.
                String[] splitted = line.trim().split("\s+");
                currentRing.add(new CoordinateXY(Float.parseFloat(splitted[0]), Float.parseFloat(splitted[1])));
            } else if (in_ring == false &&  line.strip().equals("END")) {
                // we are at the end of the whole polygon.
                break;
            }

            else if (in_ring == false && line.charAt(0) == '!') {
                // we ignore holes for now
                in_ring = true;
            }

            else if (in_ring == false) {
                // we are at the start of a polygon part.
                currentRing = new ArrayList<Coordinate>();
                in_ring = true;
            }
        }
        br.close();
        return JTS_FACTORY.createMultiPolygon(polygons.toArray(new Polygon[polygons.size()]));
    }
}
