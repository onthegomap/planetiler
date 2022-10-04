package com.onthegomap.planetiler.custommap;

import static org.junit.jupiter.api.DynamicTest.dynamicTest;

import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.custommap.validator.SchemaSpecification;
import com.onthegomap.planetiler.custommap.validator.SchemaValidator;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;

class SchemaTests {
  @TestFactory
  List<DynamicTest> shortbread() {
    return testSchema("shortbread.yml", "shortbread.spec.yml");
  }

  private List<DynamicTest> testSchema(String schema, String spec) {
    var base = Path.of("src", "main", "resources", "samples");
    var result = SchemaValidator.validate(
      SchemaConfig.load(base.resolve(schema)),
      SchemaSpecification.load(base.resolve(spec))
    );
    return result.results().stream()
      .map(test -> dynamicTest(test.example().name(), () -> {
        if (test.issues().isFailure()) {
          throw test.issues().exception();
        }
        if (!test.issues().get().isEmpty()) {
          throw new AssertionError("Validation failed:\n" + String.join("\n", test.issues().get()));
        }
      })).toList();
  }
}
