package com.onthegomap.planetiler.experimental.lua;

import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entrypoint for running a lua profile.
 */
public class LuaMain {
  private static final Logger LOGGER = LoggerFactory.getLogger(LuaMain.class);

  public static void main(String... args) throws Exception {
    LOGGER.warn(
      "Lua profiles are experimental and may change! Please provide feedback and report any bugs before depending on it in production.");
    var arguments = Arguments.fromEnvOrArgs(args);
    Path script = arguments.inputFile("script", "the lua script to run", Path.of("profile.lua"));
    LuaEnvironment.loadScript(arguments, script).run();
  }
}
