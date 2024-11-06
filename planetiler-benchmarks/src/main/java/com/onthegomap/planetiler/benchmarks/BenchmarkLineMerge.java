package com.onthegomap.planetiler.benchmarks;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.LoopLineMerger;
import com.onthegomap.planetiler.util.Try;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.operation.linemerge.LineMerger;

public class BenchmarkLineMerge {
  private static int numLines;

  public static void main(String[] args) throws Exception {
    for (int i = 0; i < 10; i++) {
      time("      JTS", geom -> {
        var lm = new LineMerger();
        lm.add(geom);
        return lm.getMergedLineStrings();
      });
      time("  loop(0)", geom -> {
        var lm = new LoopLineMerger();
        lm.setLoopMinLength(0);
        lm.setMinLength(0);
        lm.add(geom);
        return lm.getMergedLineStrings();
      });
      time("loop(0.1)", geom -> {
        var lm = new LoopLineMerger();
        lm.setLoopMinLength(0.1);
        lm.setMinLength(0.1);
        lm.add(geom);
        return lm.getMergedLineStrings();
      });
    }
    System.err.println(numLines);
  }

  private static void time(String name, Function<Geometry, Collection<LineString>> fn) throws Exception {
    System.err.println(String.join("\t",
      name,
      timeMillis(() -> fn.apply(read("mergelines_200433_lines.wkb.gz"))),
      timeMillis(() -> fn.apply(read("mergelines_239823_lines.wkb.gz"))),
      "(/s):",
      timePerSec(() -> fn.apply(read("mergelines_1759_point_line.wkb.gz"))),
      timePerSec(() -> fn.apply(makeLines(50, 2))),
      timePerSec(() -> fn.apply(makeLines(10, 10))),
      timePerSec(() -> fn.apply(makeLines(2, 50)))
    ));
  }

  private static String timePerSec(Try.SupplierThatThrows<Collection<LineString>> fn) throws Exception {
    long start = System.nanoTime();
    long end = start + Duration.ofSeconds(1).toNanos();
    int num = 0;
    for (; System.nanoTime() < end;) {
      numLines += fn.get().size();
      num++;
    }
    return Format.defaultInstance()
      .numeric(Math.round(num * 1d / ((System.nanoTime() - start) * 1d / Duration.ofSeconds(1).toNanos())), true);
  }

  private static String timeMillis(Try.SupplierThatThrows<Collection<LineString>> fn) throws Exception {
    long start = System.nanoTime();
    long end = start + Duration.ofSeconds(1).toNanos();
    int num = 0;
    for (; System.nanoTime() < end;) {
      numLines += fn.get().size();
      num++;
    }
    // equivalent of toPrecision(3)
    long nanosPer = (System.nanoTime() - start) / num;
    var bd = new BigDecimal(nanosPer, new MathContext(3));
    return Format.padRight(Duration.ofNanos(bd.longValue()).toString().replace("PT", ""), 6);
  }


  private static Geometry read(String fileName) throws IOException, ParseException {
    var path = Path.of("planetiler-core", "src", "test", "resources", "mergelines", fileName);
    byte[] bytes = Gzip.gunzip(Files.readAllBytes(path));
    return new WKBReader().read(bytes);
  }

  private static Geometry makeLines(int lines, int parts) {
    List<LineString> result = new ArrayList<>();
    double idx = 0;
    for (int i = 0; i < lines; i++) {
      Coordinate[] coords = new Coordinate[parts];
      for (int j = 0; j < parts; j++) {
        coords[j] = new CoordinateXY(idx, idx);
        idx += 0.5;
      }
      result.add(GeoUtils.JTS_FACTORY.createLineString(coords));
    }
    return new GeometryFactory().createMultiLineString(result.toArray(LineString[]::new));
  }
}
