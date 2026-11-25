package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Formats an error message to explain to a user which disk filled up and what next step might make the run succeed.
 */
public class DiskSpaceAdvice {

  private static final String MOVE_TEMPORARY_FILES = "write temporary files to another disk with --tmpdir=<dir>";
  private static final String MOVE_OUTPUT = "write the output to another disk with --output=<file>";

  private final List<Source> sources;
  private final Path tmpDir;
  private final Path output;
  private final boolean compressTemp;
  private final Function<Path, Disk> diskOf;

  /**
   * Creates advice for a run that reads {@code sources} and writes temporary files to {@code tmpDir} and the output to
   * {@code output} ({@code null} if not a local file), using {@code diskOf} to find the disk a path is on.
   */
  public DiskSpaceAdvice(List<Source> sources, Path tmpDir, Path output, boolean compressTemp,
    Function<Path, Disk> diskOf) {
    this.sources = sources;
    this.tmpDir = tmpDir;
    this.output = output;
    this.compressTemp = compressTemp;
    this.diskOf = diskOf;
  }

  /** Returns what to do when a disk is too small for the temporary files written while reading the sources. */
  public String readPhaseHint() {
    List<String> fixes = new ArrayList<>(List.of(MOVE_TEMPORARY_FILES));
    fixes.addAll(diskSavingOptions(true, List.of()));
    return freeDiskHint(fixes);
  }

  /**
   * Returns what to do when a disk is too small for the temporary files and the output archive as its being written.
   */
  public String writePhaseHint() {
    List<String> fixes = new ArrayList<>(List.of(MOVE_TEMPORARY_FILES, MOVE_OUTPUT));
    fixes.addAll(diskSavingOptions(true, sources));
    return freeDiskHint(fixes);
  }

  /**
   * Returns a message saying which disk filled up and how to make the run fit if that is why it failed with
   * {@code failure}, or empty if it failed for another reason.
   *
   * @param sourcesRead whether all sources were read before it failed, so deleting them once read would have helped
   */
  public Optional<String> explainFailure(Throwable failure, boolean sourcesRead) {
    boolean osSaysDiskFull = FileUtils.isOutOfDiskSpace(failure);
    if (!osSaysDiskFull && !Exceptions.hasCause(failure, IOException.class) &&
      !Exceptions.hasCause(failure, InternalError.class)) {
      return Optional.empty();
    }
    // The error doesn't say which file it was writing, so name the fullest disk that this run writes to.
    Map<Path, String> moveElsewhere = new LinkedHashMap<>();
    sources.stream()
      .map(source -> source.path().toAbsolutePath().normalize().getParent())
      .sorted()
      .forEach(dir -> {
        // a source directory inside another one (e.g. data/sources/natural_earth) goes under that one
        if (moveElsewhere.keySet().stream().noneMatch(dir::startsWith)) {
          moveElsewhere.put(dir, "move the sources in " + FileUtils.displayPath(dir) + " to another disk");
        }
      });
    moveElsewhere.put(tmpDir, MOVE_TEMPORARY_FILES);
    if (output != null) {
      moveElsewhere.put(output, MOVE_OUTPUT);
    }
    Map<String, Disk> disks = new LinkedHashMap<>();
    Map<String, List<String>> fixesByDisk = new LinkedHashMap<>();
    moveElsewhere.forEach((path, fix) -> {
      Disk disk = diskOf.apply(path);
      disks.putIfAbsent(disk.name(), disk);
      fixesByDisk.computeIfAbsent(disk.name(), _ -> new ArrayList<>()).add(fix);
    });
    Disk fullest = disks.values().stream().min(Comparator.comparingLong(Disk::usableBytes)).orElseThrow();
    // The operating system's message may be translated, or the disk may fill up writing a memory-mapped file
    // (InternalError), so other I/O errors also count when that disk is nearly full.
    if (!osSaysDiskFull && fullest.usableBytes() > 100_000_000) {
      return Optional.empty();
    }
    List<String> fixes = new ArrayList<>(fixesByDisk.get(fullest.name()));
    fixes.addAll(diskSavingOptions(
      isOn(tmpDir, fullest),
      sourcesRead ? sources.stream().filter(source -> isOn(source.path(), fullest)).toList() : List.of()));
    return Optional.of("Ran out of disk space on " + fullest.name() + ". " + freeDiskHint(fixes));
  }

  private boolean isOn(Path path, Disk disk) {
    return diskOf.apply(path).name().equals(disk.name());
  }

  /**
   * Returns the options that would make this run use less disk: compressing temporary files if they're on the disk that
   * is too small, and deleting the biggest of {@code candidates} once it's read.
   */
  private List<String> diskSavingOptions(boolean forTemporaryFiles, List<Source> candidates) {
    List<String> result = new ArrayList<>();
    if (forTemporaryFiles && !compressTemp) {
      result.add("use --compress-temp for smaller temporary files");
    }
    candidates.stream()
      .filter(source -> !source.freeAfterReading() && Files.isRegularFile(source.path()))
      .max(Comparator.comparingLong(source -> FileUtils.size(source.path())))
      .ifPresent(source -> result.add("use --free-%s-after-read to delete %s once it's read".formatted(
        source.id().replace('_', '-'), FileUtils.displayPath(source.path()))));
    return result;
  }

  private static String freeDiskHint(List<String> fixes) {
    return "Free up space on that disk" +
      (fixes.isEmpty() ? "." : ", or:" + fixes.stream().map(fix -> "\n  - " + fix).collect(Collectors.joining()));
  }

  /** A source file a run reads, and whether it gets deleted once read ({@code --free-<id>-after-read}). */
  public record Source(String id, Path path, boolean freeAfterReading) {}

  /** A disk, with the bytes left on it for this process to write. Paths on the same disk give the same name. */
  public record Disk(String name, long usableBytes) {

    /** Returns the disk that {@code path} is on, or would be on once created. */
    public static Disk of(Path path) {
      FileStore store = FileUtils.getFileStore(path);
      return new Disk(store.toString(), FileUtils.usableSpace(store));
    }
  }
}
