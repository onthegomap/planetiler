package com.onthegomap.planetiler.contour;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.Downloader;
import java.io.IOException;

public class EsaWorldcover {
  static String getCoord(double ilon, double ilat) {
    int lon = (int) (Math.floor(ilon / 3) * 3);
    int lat = (int) (Math.floor(ilat / 3) * 3);
    String latString = lat >= 0 ? "N%02d".formatted(lat) : "S%02d".formatted(-lat);
    String lonString = lon >= 0 ? "E%03d".formatted(lon) : "W%03d".formatted(-lon);
    return latString + lonString;

  }

  public static void main(String[] args) throws IOException {
    var config = PlanetilerConfig.defaults();
    String entry = "ESA_WorldCover_10m_2021_v200_" + getCoord(-71.5, 42.2) + "_Map.tif";
    String url = "https://esa-worldcover.s3.amazonaws.com/v200/2021/map/" + entry;
    // ESA_WorldCover_10m_2021_v200_N42W072_Map.tif
    System.err.println("Getting " + url);
    try (var is = Downloader.openStream(url, config)) {
      System.err.println(is.readAllBytes().length);
    }
  }
}
