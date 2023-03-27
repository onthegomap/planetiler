package com.onthegomap.planetiler.contour;

import static java.util.Map.entry;

import com.carrotsearch.hppc.IntIntHashMap;
import com.carrotsearch.hppc.IntIntMap;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import javax.imageio.ImageIO;

public class EsaWorldcover {
  // stack:
  // nothing / built-up / moss / water

  // 1: shrub / grass / herbacious wetland
  // 2: crop
  // 3: tree / mangrove
  // 4: very tree / mangrove

  // ice ?
  private static final Map<Integer, String> colors = Map.ofEntries(
    entry(new Color(0, 100, 0, 255).getRGB(), "treecover"),
    entry(new Color(255, 187, 34, 255).getRGB(), "shrubland"),
    entry(new Color(255, 255, 76, 255).getRGB(), "grassland"),
    entry(new Color(240, 150, 255, 255).getRGB(), "cropland"),
    entry(new Color(250, 0, 0, 255).getRGB(), "builtup"),
    entry(new Color(180, 180, 180, 255).getRGB(), "bare"),
    entry(new Color(240, 240, 240, 255).getRGB(), "snow"),
    entry(new Color(0, 100, 200, 255).getRGB(), "water"),
    entry(new Color(0, 150, 160, 255).getRGB(), "wetland"),
    entry(new Color(0, 207, 117, 255).getRGB(), "mangrove"),
    entry(new Color(250, 230, 160, 255).getRGB(), "moss")
  );

  static String getCoord(double ilon, double ilat) {
    int lon = (int) (Math.floor(ilon / 3) * 3);
    int lat = (int) (Math.floor(ilat / 3) * 3);
    String latString = lat >= 0 ? "N%02d".formatted(lat) : "S%02d".formatted(-lat);
    String lonString = lon >= 0 ? "E%03d".formatted(lon) : "W%03d".formatted(-lon);
    return latString + lonString;

  }

  public static void main(String[] args) throws IOException {
    var config = PlanetilerConfig.defaults();
    String entry = "ESA_WorldCover_10m_2021_v200_" + getCoord(-72, 42) + "_Map.tif";
    String url = "https://esa-worldcover.s3.amazonaws.com/v200/2021/map/" + entry;
    // ESA_WorldCover_10m_2021_v200_N42W072_Map.tif
    System.err.println("Getting " + url);
    BufferedImage image = ImageIO.read(new URL(url));
    System.err.println("width=" + image.getWidth());
    System.err.println("height=" + image.getHeight());
    IntIntMap counts = new IntIntHashMap();
    int idx = 0;

    int[] arr = new int[image.getWidth() * image.getHeight()];
    image.getRGB(0, 0, image.getWidth(), image.getHeight(), arr, 0, image.getWidth());
    System.err.println("got pixels");
    for (int y = 0; y < image.getHeight(); y++) {
      for (int x = 0; x < image.getWidth(); x++) {
        int color = arr[idx];
        counts.put(color, counts.getOrDefault(color, 0) + 1);
        idx++;
      }
    }
    for (var e : counts.keys()) {
      System.err.println(e.value + " " + colors.get(e.value) + " " +
        ((counts.get(e.value) * 100L) / ((long) image.getWidth() * image.getHeight())) + "%");
    }
  }
}
