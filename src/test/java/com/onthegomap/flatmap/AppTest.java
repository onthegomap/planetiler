package com.onthegomap.flatmap;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class AppTest {

  @Test
  public void testFast() {
    assertEquals("hello world", new App().getGreeting());
  }
}
