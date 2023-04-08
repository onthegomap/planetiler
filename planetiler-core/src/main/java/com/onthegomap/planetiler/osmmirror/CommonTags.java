package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class CommonTags {
  // (curl 'https://taginfo.openstreetmap.org/api/4/tags/popular?sortname=count_all&sortorder=desc&page=1&rp=999&qtype=tag' | jq -r '.data[].key + "\n" + .data[].value') | awk '!x[$0]++' > planetiler-core/src/main/resources/common_tags.txt
  // (curl 'https://taginfo.openstreetmap.org/api/4/tags/popular?sortname=count_all&sortorder=desc&page=1&rp=999&qtype=tag' | jq -r '.data[].key + "\n" + .data[].value'; curl 'https://taginfo.openstreetmap.org/api/4/keys/all?include=prevalent_values&sortname=count_all&sortorder=desc&page=1&rp=999&qtype=key' | jq -r '.data[].key') | sort | uniq > planetiler-core/src/main/resources/common_tags.txt
  private final String[] forward;
  private final ObjectIntMap<String> reverse;

  public CommonTags() {
    try (var is = CommonTags.class.getResourceAsStream("/common_tags.txt")) {
      forward = new String(Objects.requireNonNull(is).readAllBytes(), StandardCharsets.UTF_8).split("\n");
      reverse = new ObjectIntHashMap<>(forward.length);
      for (int i = 0; i < forward.length; i++) {
        reverse.putIfAbsent(forward[i], i);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public String decode(int key) {
    return forward[key];
  }

  public int encode(String string) {
    return reverse.getOrDefault(string, -1);
  }
}
