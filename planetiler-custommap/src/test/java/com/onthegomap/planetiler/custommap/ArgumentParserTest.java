package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.expression.ParseException;
import java.util.Map;
import org.junit.jupiter.api.Test;

class ArgumentParserTest {

  @Test
  void testParseEmptyArgs() {
    Map<String, String> cliArgs = Map.of();
    Map<String, Object> schemaArgs = Map.of();
    ArgumentParser.buildRootContext(Arguments.of(cliArgs), schemaArgs);
  }

  @Test
  void setPlanetilerConfigThroughCli() {
    Map<String, String> cliArgs = Map.of("threads", "9999");
    Map<String, Object> schemaArgs = Map.of("threads", "8888");
    var result = ArgumentParser.buildRootContext(Arguments.of(cliArgs), schemaArgs);
    assertEquals(9999, result.config().threads());
  }

  @Test
  void setPlanetilerConfigThroughSchemaArgs() {
    assertEquals(8888, ArgumentParser.buildRootContext(
      Arguments.of(Map.of()),
      Map.of("threads", "8888")
    ).config().threads());
    assertEquals(8888, ArgumentParser.buildRootContext(
      Arguments.of(Map.of()),
      Map.of("threads", 8888)
    ).config().threads());
  }

  @Test
  void configDefinesDefaultArgumentValue() {
    assertEquals(8888, ArgumentParser.buildRootContext(
      Arguments.of(Map.of()),
      Map.of(
        "arg", 8888
      )
    ).argument("arg"));
  }

  @Test
  void overrideDefaultArgFromConfig() {
    assertEquals(9999, ArgumentParser.buildRootContext(
      Arguments.of(Map.of("arg", "9999")),
      Map.of(
        "arg", 8888
      )
    ).argument("arg"));
  }

  @Test
  void defineArgTypeInConfig() {
    assertEquals(8888, ArgumentParser.buildRootContext(
      Arguments.of(),
      Map.of(
        "arg", Map.of(
          "default", "8888",
          "type", "integer"
        )
      )
    ).argument("arg"));
  }

  @Test
  void implyTypeFromDefaultValue() {
    assertEquals("8888", ArgumentParser.buildRootContext(
      Arguments.of(),
      Map.of(
        "arg", "8888"
      )
    ).argument("arg"));
    assertEquals("9999", ArgumentParser.buildRootContext(
      Arguments.of("arg", "9999"),
      Map.of(
        "arg", "8888"
      )
    ).argument("arg"));

    assertEquals(8888, ArgumentParser.buildRootContext(
      Arguments.of(),
      Map.of(
        "arg", 8888
      )
    ).argument("arg"));
    assertEquals(9999, ArgumentParser.buildRootContext(
      Arguments.of("arg", "9999"),
      Map.of(
        "arg", 8888
      )
    ).argument("arg"));
  }

  @Test
  void computeDefaultValueUsingExpression() {
    assertEquals(2, ArgumentParser.buildRootContext(
      Arguments.of(),
      Map.of(
        "arg", Map.of(
          "default", "${ 1+1 }",
          "type", "integer"
        )
      )
    ).argument("arg"));
  }

  @Test
  void implyTypeFromExpressionValue() {
    assertEquals(2L, ArgumentParser.buildRootContext(
      Arguments.of(),
      Map.of(
        "arg", "${ 1+1 }"
      )
    ).argument("arg"));
  }

  @Test
  void referenceOtherArgInExpression() {
    Map<String, Object> configArgs = Map.of(
      "arg1", 1,
      "arg2", "${ args.arg1 + 1}"
    );
    assertEquals(2L, ArgumentParser.buildRootContext(
      Arguments.of(),
      configArgs
    ).argument("arg2"));
    assertEquals(3L, ArgumentParser.buildRootContext(
      Arguments.of(Map.of("arg1", "2")),
      configArgs
    ).argument("arg2"));
    assertEquals(10L, ArgumentParser.buildRootContext(
      Arguments.of(Map.of("arg2", "10")),
      configArgs
    ).argument("arg2"));
  }

  @Test
  void referenceOtherArgInExpressionTwice() {
    Map<String, Object> configArgs = Map.of(
      "arg1", 1,
      "arg2", "${ args.arg1 + 1}",
      "arg3", "${ args.arg2 + 1}"
    );
    assertEquals(3L, ArgumentParser.buildRootContext(
      Arguments.of(),
      configArgs
    ).argument("arg3"));
  }

  @Test
  void failOnInfiniteLoop() {
    Map<String, Object> configArgs = Map.of(
      "arg1", Map.of(
        "default", "${ args.arg3 + 1 }",
        "type", "long"
      ),
      "arg2", "${ args.arg1 + 1}",
      "arg3", "${ args.arg2 + 1}"
    );
    var empty = Arguments.of();
    assertThrows(ParseException.class, () -> ArgumentParser.buildRootContext(
      empty,
      configArgs
    ));
    // but if you break the chain it's OK?
    assertEquals(3L, ArgumentParser.buildRootContext(
      Arguments.of(Map.of("arg1", "1")),
      configArgs
    ).argument("arg3"));
  }

  @Test
  void setPlanetilerConfigFromOtherArg() {
    assertEquals(8888, ArgumentParser.buildRootContext(
      Arguments.of(Map.of()),
      Map.of(
        "other", "8888",
        "threads", "${ args.other }"
      )
    ).config().threads());
  }

  @Test
  void testCantRedefineBuiltin() {
    var fromCli = Arguments.of(Map.of());
    Map<String, Object> fromConfig = Map.of(
      "threads", Map.of(
        "default", 4,
        "type", "string"
      )
    );
    assertThrows(ParseException.class, () -> ArgumentParser.buildRootContext(fromCli, fromConfig));
  }

  @Test
  void testDefineRequiredArg() {
    var argsWithoutValue = Arguments.of(Map.of());
    var argsWithValue = Arguments.of(Map.of("arg", "3"));
    Map<String, Object> fromConfig = Map.of(
      "arg", Map.of(
        "type", "integer",
        "description", "desc"
      )
    );
    assertThrows(ParseException.class, () -> ArgumentParser.buildRootContext(argsWithoutValue, fromConfig));
    assertEquals(3, ArgumentParser.buildRootContext(argsWithValue, fromConfig).argument("arg"));
  }

  @Test
  void setPlanetilerConfigFromOtherPlanetilerConfig() {
    var root = ArgumentParser.buildRootContext(
      Arguments.of(Map.of()),
      Map.of(
        "mmap", "${ args.threads < 1 }"
      )
    );
    assertTrue(root.config().mmapTempStorage());
  }
}
