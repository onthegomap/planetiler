package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.LongConsumer;

public class CountingOutputStream extends OutputStream {

  private final OutputStream wrapped;
  private final LongConsumer writtenBytesConsumer;

  public CountingOutputStream(OutputStream wrapped, LongConsumer writtenBytesConsumer) {
    this.wrapped = wrapped;
    this.writtenBytesConsumer = writtenBytesConsumer;
  }

  @Override
  public void write(int i) throws IOException {
    wrapped.write(i);
    writtenBytesConsumer.accept(1L);
  }

  @Override
  public void write(byte[] b) throws IOException {
    wrapped.write(b);
    writtenBytesConsumer.accept(b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    wrapped.write(b, off, len);
    writtenBytesConsumer.accept(len);
  }

  @Override
  public void flush() throws IOException {
    wrapped.flush();
  }

  @Override
  public void close() throws IOException {
    wrapped.close();
  }
}
