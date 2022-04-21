package com.onthegomap.planetiler.reader;

/**
 * Error encountered while parsing an input file.
 */
public class FileFormatException extends RuntimeException {
  public FileFormatException(String message) {
    super(message);
  }

  public FileFormatException(String message, Throwable throwable) {
    super(message, throwable);
  }
}
