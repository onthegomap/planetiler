package com.onthegomap.planetiler.custommap;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonParser {

  public static Set<String> extractStringSet(JsonNode json) {
    Set<String> set = new HashSet<>();
    for (int i = 0; i < json.size(); i++) {
      set.add(json.get(i).asText());
    }
    return set;
  }

}
