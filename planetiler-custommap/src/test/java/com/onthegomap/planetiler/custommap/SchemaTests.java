package com.onthegomap.planetiler.custommap;

import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.custommap.configschema.SchemaConfig;
import com.onthegomap.planetiler.validator.SchemaSpecification;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.TestFactory;

class SchemaTests {
  @TestFactory
  Stream<DynamicNode> shortbread() {
    return test("shortbread.yml", "shortbread.spec.yml");
  }

  private static Stream<DynamicNode> test(String schemaFile, String specFile) {
    var base = Path.of("src", "main", "resources", "samples");
    SchemaConfig schema = SchemaConfig.load(base.resolve(schemaFile));
    SchemaSpecification specification = SchemaSpecification.load(base.resolve(specFile));
    var context = Contexts.buildRootContext(Arguments.of().silence(), schema.args());
    var profile = new ConfiguredProfile(schema, context);
    return TestUtils.validateProfile(profile, specification);
  }
}
