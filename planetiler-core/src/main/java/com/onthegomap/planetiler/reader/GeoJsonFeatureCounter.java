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
        if (token == JsonToken.START_ARRAY) {
          parser.skipChildren();
        } else if (token == JsonToken.FIELD_NAME) {
          String name = parser.currentName();
          parser.nextToken();
          if ("geometry".equals(name)) {
            parser.skipChildren();
            count++;
          } else if (!"features".equals(name)) {
            parser.skipChildren();
          }
        }
      }
    }
    return count;
  }
}
