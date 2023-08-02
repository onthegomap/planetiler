package org.apache.hadoop.conf;

public class Configuration {

  public boolean getBoolean(String x, boolean y) {
    return switch (x) {
      case "parquet.avro.readInt96AsFixed" -> true;
      default -> y;
    };
  }

  public void setBoolean(String x, boolean y) {}

  public int getInt(String x, int y) {
    return y;
  }

  public String get(String x) {
    return null;
  }

  public String[] getStrings(String x, String[] strings) {
    return new String[0];
  }
}
