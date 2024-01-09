package com.onthegomap.planetiler.util;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

class CloseShieldOutputStreamTest {

  @Test
  void test() throws IOException {
    final OutputStream delegate = mock(OutputStream.class);
    final OutputStream os = new CloseShieldOutputStream(delegate);

    os.close();
    verifyNoMoreInteractions(delegate);

    os.write(1);
    verify(delegate).write(1);
    verifyNoMoreInteractions(delegate);

    os.write(new byte[]{2});
    verify(delegate).write(new byte[]{2});
    verifyNoMoreInteractions(delegate);

    os.write(new byte[]{3}, 4, 5);
    verify(delegate).write(new byte[]{3}, 4, 5);
    verifyNoMoreInteractions(delegate);

    os.flush();
    verify(delegate).flush();
    verifyNoMoreInteractions(delegate);
  }

}
