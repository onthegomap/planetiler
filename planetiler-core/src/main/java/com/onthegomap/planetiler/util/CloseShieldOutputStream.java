package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * {@link OutputStream} decorator that suppresses {@link #close()}.
 */
public class CloseShieldOutputStream extends DelegatingOutputStream {

  public CloseShieldOutputStream(OutputStream wrapped) {
    super(wrapped);
  }

  @Override
  public void close() throws IOException {
    // suppress closing
  }
}
