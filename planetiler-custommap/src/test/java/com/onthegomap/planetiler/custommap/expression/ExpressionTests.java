package com.onthegomap.planetiler.custommap.expression;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.Contexts;
import java.util.Map;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class ExpressionTests {
  @ParameterizedTest
  @CsvSource(value = {
    "1|1|long",
    "1+1|2|long",
    "'1' + string(1)|11|string",

    "coalesce(null, null)||null",
    "coalesce(null, 1)|1|long",
    "coalesce(1, null)|1|long",
    "coalesce(1, 2)|1|long",
    "coalesce(null, null, 1)|1|long",
    "coalesce(null, 1, 2)|1|long",
    "coalesce(1, 2, null)+2|3|long",

    "nullif('abc', '')|'abc'|string",
    "nullif('', '')|null|null",
    "nullif(1, 1)|null|null",
    "nullif(1, 12)|1|long",

    "'123'.replace('12', 'X')|X3|string",
    "'123'.replaceRegex('1(.)', '$1')|23|string",
    "string(123)|123|string",
    "string({1:2,3:'4'}[1])|2|string",

    "'abc'.matches('a.c')|true|boolean",
    "'abc'.matches('a.d')|false|boolean",

    "{'a': 1}.has('a')|true|boolean",
    "{'a': 1}.has('a', 1)|true|boolean",
    "{'a': 1}.has('a', 1, 2)|true|boolean",
    "{'a': 2}.has('a', 1, 2)|true|boolean",
    "{'a': 2}.has('a', 3)|false|boolean",
    "{'a': 1}.has('b')|false|boolean",
    "int({'tags': {'wikidata': 'Q1'}}.tags.wikidata.replace('Q', ''))|1|long",

    "coalesce({'a': 1}.get('a'), 2)|1|long",
    "coalesce({'a': 1}.get('b'), 2)|2|long",
    "{'a': 1}.getOrDefault('a', 2)|1|long",
    "{'a': 1}.getOrDefault('b', 2)|2|long",

    "max([1, 2, 3])|3|long",
    "max([1.1, 2.2, 3.3])|3.3|double",
    "min([1, 2, 3])|1|long",
    "min([1.1, 2.2, 3.3])|1.1|double",
    "max([1])|1|long",
    "min([1])|1|long",

    "args.arg_from_config|1|long",
    "args.arg_from_config + 1|2|long",
    "args[\"arg_from_config\"]|1|long",
    "args[\"arg_from_config\"] + 1|2|long",
    "args.overridden_arg_from_config|4|long",
    "args[\"overridden_arg_from_config\"] + 1|5|long",
    "args.arg_from_cli|2|string",
    "args[\"arg_from_cli\"]|2|string",
  }, delimiter = '|')
  void testExpression(String in, String expected, String type) {
    Map<String, Object> configFromSchema = Map.of("arg_from_config", 1, "overridden_arg_from_config", 3);
    Map<String, String> configFromCli = Map.of("arg_from_cli", "2", "overridden_arg_from_config", "4");
    var context = Contexts.buildRootContext(
      Arguments.of(configFromCli),
      configFromSchema
    );
    var expression = ConfigExpressionScript.parse(in, context.description());
    var result = expression.apply(context);
    switch (type) {
      case "long" -> assertEquals(Long.valueOf(expected), result);
      case "double" -> assertEquals(Double.valueOf(expected), result);
      case "string" -> assertEquals(expected, result);
      case "boolean" -> assertEquals(Boolean.valueOf(expected), result);
      case "null" -> assertNull(result);
      default -> throw new IllegalArgumentException(type);
    }
  }
}
