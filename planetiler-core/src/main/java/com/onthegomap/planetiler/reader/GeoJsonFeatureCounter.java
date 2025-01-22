package com.onthegomap.planetiler.reader;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import java.io.IOException;
import java.io.InputStream;

public class GeoJsonFeatureCounter {

  public static long count(InputStream inputStream) throws IOException {
    long count = 0;
    try (
      JsonParser parser =
        new JsonFactory().enable(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION).createParser(inputStream)
    ) {
      while (!parser.isClosed()) {
        JsonToken token = parser.nextToken();
        if (token == JsonToken.FIELD_NAME) {
          String name = parser.currentName();
          if ("geometry".equals(name)) {
            parser.nextToken();
            parser.skipChildren();
            count++;
          } else if ("properties".equals(name)) {
            parser.nextToken();
            parser.skipChildren();
          }
        }
      }
    }
    return count;
  }
}
