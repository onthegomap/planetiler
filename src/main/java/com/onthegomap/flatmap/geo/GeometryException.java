package com.onthegomap.flatmap.geo;

public class GeometryException extends Exception {

  private final String stat;

  public GeometryException(String stat, String message, Throwable cause) {
    super(message, cause);
    this.stat = stat;
  }

  public GeometryException(String stat, String message) {
    super(message);
    this.stat = stat;
  }

  public String stat() {
    return stat;
  }
}
