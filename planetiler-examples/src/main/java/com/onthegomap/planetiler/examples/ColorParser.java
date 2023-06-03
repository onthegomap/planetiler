package com.onthegomap.planetiler.examples;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class ColorParser {
  private final HashMap<String, Integer> colorList = new HashMap<>();

  ColorParser() {
    initColorList();
  }

  private void initColorList() {
    String data = "";

    try {
      InputStream inputStream = ColorParser.class.getResourceAsStream("/colors.json");
      data = readFromInputStream(inputStream);
    } catch (IOException e) {
      e.printStackTrace();
    }

    JsonElement jsonElement = JsonParser.parseString(data);
    JsonObject json = jsonElement.getAsJsonObject();

    for (String key : json.keySet()) {
      JsonElement value = json.get(key);
      JsonArray components = value.getAsJsonArray();

      int r = components.get(0).getAsInt();
      int g = components.get(1).getAsInt();
      int b = components.get(2).getAsInt();

      int color = r * 256 * 256 + g * 256 + b;

      colorList.put(key, color);
    }
  }

  private static String readFromInputStream(InputStream inputStream) throws IOException {
    StringBuilder resultStringBuilder = new StringBuilder();

    try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
      String line;
      while ((line = br.readLine()) != null) {
        resultStringBuilder.append(line).append("\n");
      }
    }
    return resultStringBuilder.toString();
  }

  private int[] hexToRgb(String hex) {
    java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(
      "^#?([a-f\\d]{2})([a-f\\d]{2})([a-f\\d]{2})$",
      java.util.regex.Pattern.CASE_INSENSITIVE
    );
    java.util.regex.Matcher matcher = pattern.matcher(hex);

    if (matcher.find()) {
      int r = Integer.parseInt(matcher.group(1), 16);
      int g = Integer.parseInt(matcher.group(2), 16);
      int b = Integer.parseInt(matcher.group(3), 16);
      return new int[]{r, g, b};
    }

    return null;
  }

  public Integer parseColor(String str) {
    if (str == null || str.length() == 0) {
      return null;
    }

    String noSpacesLowerCase = str.replaceAll("[ _-]", "").toLowerCase();
    Integer colorListValue = colorList.get(noSpacesLowerCase);

    if (colorListValue != null) {
      return colorListValue;
    }

    String hex = str.contains(";") ? str.split(";")[0] : str;
    int[] components = hexToRgb(hex);

    if (components != null) {
      return components[0] * 256 * 256 + components[1] * 256 + components[2];
    }

    return null;
  }
}
