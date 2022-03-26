package com.onthegomap.planetiler.custommap;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;

public class JsonParser {

  public static Set<String> extractStringSet(JsonNode json) {
    if (json == null) {
      return Collections.emptySet();
    }
    Set<String> set = new HashSet<>();
    for (int i = 0; i < json.size(); i++) {
      set.add(json.get(i).asText());
    }
    return set;
  }

  public static String getStringField(JsonNode json, String field) {
    JsonNode val = json.get(field);
    if (val == null) {
      return null;
    }
    return val.asText();
  }

  public static Integer getIntField(JsonNode json, String field) {
    JsonNode val = json.get(field);
    if (val == null) {
      return null;
    }
    return val.asInt();
  }

  public static Double getDoubleField(JsonNode json, String field) {
    if (json == null) {
      return null;
    }
    JsonNode val = json.get(field);
    if (val == null) {
      return null;
    }
    return val.asDouble();
  }

}
