package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.OutputStream;

abstract class DelegatingOutputStream extends OutputStream {

  private final OutputStream delegate;

  protected DelegatingOutputStream(OutputStream wrapped) {
    this.delegate = wrapped;
  }

  @Override
  public void write(int i) throws IOException {
    delegate.write(i);
  }

  @Override
  public void write(byte[] b) throws IOException {
    delegate.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    delegate.write(b, off, len);
  }

  @Override
  public void flush() throws IOException {
    delegate.flush();
  }

  @Override
  public void close() throws IOException {
    delegate.close();
  }
}
