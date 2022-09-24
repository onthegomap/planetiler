package com.onthegomap.planetiler.custommap.expression.stdlib;

import java.util.List;
import org.projectnessie.cel.EnvOption;
import org.projectnessie.cel.Library;
import org.projectnessie.cel.ProgramOption;
import org.projectnessie.cel.interpreter.functions.Overload;

/** Creates a {@link Library} of built-in functions that can be made available to dynamic expressions. */
class PlanetilerLib implements Library {

  private final List<BuiltInFunction> builtInFunctions;

  PlanetilerLib(List<BuiltInFunction> builtInFunctions) {
    this.builtInFunctions = builtInFunctions;
  }

  @Override
  public List<EnvOption> getCompileOptions() {
    return List.of(EnvOption.declarations(
      builtInFunctions.stream().map(BuiltInFunction::signature).toList()
    ));
  }

  @Override
  public List<ProgramOption> getProgramOptions() {
    return List.of(ProgramOption.functions(
      builtInFunctions.stream().flatMap(b -> b.implementations().stream()).toArray(Overload[]::new)
    ));
  }
}
