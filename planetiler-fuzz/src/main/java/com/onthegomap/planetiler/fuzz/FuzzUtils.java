package com.onthegomap.planetiler.fuzz;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

/** Utilities shared by fuzzers. */
public class FuzzUtils {

  private FuzzUtils() {}

  /**
   * Returns a file from {@code planetiler-core/src/test/resources} so fuzzers can run from an IDE with no downloads.
   * <p>
   * Looks in the repo these classes were built in, then in the working directory and its parents.
   */
  public static Path testResource(String name) {
    // an IDE may run from another working directory, so start from where the classes are (planetiler-fuzz/target/...)
    return Stream.of(codeLocation(), Path.of("").toAbsolutePath())
      .flatMap(dir -> Stream.iterate(dir, Objects::nonNull, Path::getParent))
      .map(dir -> dir.resolve(Path.of("planetiler-core", "src", "test", "resources", name)))
      .filter(Files::exists)
      .findFirst()
      .orElseThrow(() -> new IllegalStateException("Run from the planetiler repo, could not find " + name));
  }

  private static Path codeLocation() {
    try {
      return Path.of(FuzzUtils.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toAbsolutePath();
    } catch (URISyntaxException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Runs {@code mainClass} with {@code args} in a new JVM with the same classpath and heap settings, writing its output
   * to {@code log}.
   *
   * @throws IllegalStateException if it exits with a nonzero code
   */
  public static void runInNewJvm(String mainClass, List<String> args, Path log)
    throws IOException, InterruptedException {
    List<String> command = new ArrayList<>();
    command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
    ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
      .filter(arg -> arg.startsWith("-Xmx") || arg.startsWith("-Xms"))
      .forEach(command::add);
    command.addAll(List.of("-cp", System.getProperty("java.class.path"), mainClass));
    command.addAll(args);
    int exitCode = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start().waitFor();
    if (exitCode != 0) {
      // last 30 lines should contain the failure reason
      List<String> lines = Files.readAllLines(log);
      String tail = String.join(System.lineSeparator(), lines.subList(Math.max(0, lines.size() - 30), lines.size()));
      throw new IllegalStateException(
        mainClass + " exited with " + exitCode + ", last lines of " + log + ":" + System.lineSeparator() + tail);
    }
  }
}
