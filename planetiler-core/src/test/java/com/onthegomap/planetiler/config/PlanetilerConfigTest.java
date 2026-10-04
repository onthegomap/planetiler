package com.onthegomap.planetiler.config;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class PlanetilerConfigTest {

  @Test
  void testSourceParallelismDefaultsToOne() {
    assertEquals(1, PlanetilerConfig.defaults().sourceParallelism());
  }

  @Test
  void testSourceParallelismParsesArgument() {
    assertEquals(4, PlanetilerConfig.from(Arguments.fromArgs("--source_parallelism=4")).sourceParallelism());
  }

  @Test
  void testSourceParallelismClampedToOne() {
    assertEquals(1, PlanetilerConfig.from(Arguments.fromArgs("--source_parallelism=0")).sourceParallelism());
    assertEquals(1, PlanetilerConfig.from(Arguments.fromArgs("--source_parallelism=-5")).sourceParallelism());
  }
}
