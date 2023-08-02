package com.onthegomap.planetiler.overture;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.AwsOsm;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OvertureUrls {
  private static final String BASE = "https://overturemaps-us-west-2.s3.amazonaws.com";


  public static List<String> getAll(PlanetilerConfig config, String prefix) {
    return AwsOsm.s3List(config, BASE + "/" + prefix).stream()
      .map(d -> BASE + "/" + d.key())
      .toList();
  }

  public static List<String> sampleSmallest(PlanetilerConfig config, String prefix) {
    var smallestToLargest = AwsOsm.s3List(config, BASE + "/" + prefix).stream()
      .sorted(Comparator.comparing(AwsOsm.ContentXml::size))
      .map(d -> BASE + "/" + d.key())
      .toList();
    Map<String, String> smallests = new HashMap<>();
    for (var url : smallestToLargest) {
      var key = url.contains("admins") ? url : url.replaceAll("/[^/]*$", "");
      smallests.putIfAbsent(key, url);
    }
    return smallests.values().stream().toList();
  }
}
