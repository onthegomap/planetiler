package com.onthegomap.planetiler.experimental.lua;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.validator.SchemaSpecification;
import java.io.IOException;
import java.util.Objects;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

class LuaProfilesTest {

  @TestFactory
  Stream<DynamicNode> testPower() throws IOException {
    return validate("power.lua");
  }

  @TestFactory
  Stream<DynamicNode> testRoadsMain() throws IOException {
    return validate("roads_main.lua");
  }

  @TestFactory
  Stream<DynamicNode> testRoads() throws IOException {
    return validate("roads.lua");
  }

  @TestFactory
  Stream<DynamicNode> testMultifile() throws IOException {
    return validate("multifile.lua");
  }

  private static String readResource(String resource) throws IOException {
    try (var is = LuaProfilesTest.class.getResourceAsStream(resource)) {
      return new String(Objects.requireNonNull(is).readAllBytes());
    }
  }

  private static Stream<DynamicNode> validate(String name) throws IOException {
    return validate(name, null);
  }

  private static Stream<DynamicNode> validate(String name, String spec) throws IOException {
    LuaEnvironment env = LuaEnvironment.loadScript(Arguments.of(), readResource("/" + name), name);
    return TestUtils.validateProfile(
      env.profile,
      SchemaSpecification.load(readResource("/" + (spec != null ? spec : env.planetiler.examples)))
    );
  }
}
