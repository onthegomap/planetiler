package com.onthegomap.planetiler.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import java.util.stream.Stream;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

class HashingTest {

  @ParameterizedTest
  @ArgumentsSource(TestArgs.class)
  void testFnv32(boolean expectSame, byte[] data0, byte[] data1) {
    var hash0 = Hashing.fnv32(data0);
    var hash1 = Hashing.fnv32(data1);
    if (expectSame) {
      assertEquals(hash0, hash1);
    } else {
      assertNotEquals(hash0, hash1);
    }
  }

  public static class TestArgs implements ArgumentsProvider {

    @Override
    public Stream<? extends Arguments> provideArguments(ExtensionContext context) throws Exception {
      return Stream.of(
        argsOf(true, new byte[]{}, new byte[]{}),
        argsOf(true, new byte[]{1}, new byte[]{1}),
        argsOf(true, new byte[]{1, 2}, new byte[]{1, 2}),
        argsOf(false, new byte[]{1}, new byte[]{2}),
        argsOf(false, new byte[]{1}, new byte[]{1, 1})
      );
    }

    private static Arguments argsOf(boolean expectSame, byte[] data0, byte[] data1) {
      return Arguments.of(expectSame, data0, data1);
    }

  }

}
