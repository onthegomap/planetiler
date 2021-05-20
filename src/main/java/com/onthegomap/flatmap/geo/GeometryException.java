package com.onthegomap.flatmap.geo;

public class GeometryException extends Exception {

  public GeometryException(Throwable cause) {
    super(cause);
  }

  public GeometryException(String message, Throwable cause) {
    super(message, cause);
  }

  public GeometryException(String message) {
    super(message);
  }

}
