package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import com.onthegomap.planetiler.stats.Counter;
import java.io.IOException;
import java.io.OutputStream;
import org.junit.jupiter.api.Test;

class CountingOutputStreamTest {

  @Test
  void test() throws IOException {

    final OutputStream delegate = mock(OutputStream.class);
    final var c = Counter.newSingleThreadCounter();
    final OutputStream os = new CountingOutputStream(delegate, c::incBy);

    os.close();
    verify(delegate).close();
    assertEquals(0, c.get());

    os.write(1);
    verify(delegate).write(1);
    verifyNoMoreInteractions(delegate);
    assertEquals(1L, c.get());

    os.write(new byte[]{2, 3});
    verify(delegate).write(new byte[]{2, 3});
    verifyNoMoreInteractions(delegate);
    assertEquals(1L + 2L, c.get());

    os.write(new byte[]{4, 5, 6}, 7, 8);
    verify(delegate).write(new byte[]{4, 5, 6}, 7, 8);
    verifyNoMoreInteractions(delegate);
    assertEquals(1L + 2L + 8L, c.get());

    os.flush();
    verify(delegate).flush();
    verifyNoMoreInteractions(delegate);
    assertEquals(1L + 2L + 8L, c.get());
  }

}
