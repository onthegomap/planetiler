package com.onthegomap.planetiler.reader.osm;

import static com.onthegomap.planetiler.reader.osm.PolyFileReader.parsePolyFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.reader.FileFormatException;
import java.io.IOException;
import org.junit.jupiter.api.Test;

class PolyFileReaderTest {

  @Test
  void testParseAustralia() throws Exception {
    var poly = parsePolyFile("""
      australia_v
      first_area
           0.1446693E+03    -0.3826255E+02
           0.1446627E+03    -0.3825661E+02
           0.1446763E+03    -0.3824465E+02
           0.1446813E+03    -0.3824343E+02
           0.1446824E+03    -0.3824484E+02
           0.1446826E+03    -0.3825356E+02
           0.1446876E+03    -0.3825210E+02
           0.1446919E+03    -0.3824719E+02
           0.1447006E+03    -0.3824723E+02
           0.1447042E+03    -0.3825078E+02
           0.1446758E+03    -0.3826229E+02
           0.1446693E+03    -0.3826255E+02
      END
      second_area
           0.1422436E+03    -0.3839315E+02
           0.1422496E+03    -0.3839070E+02
           0.1422543E+03    -0.3839025E+02
           0.1422574E+03    -0.3839155E+02
           0.1422467E+03    -0.3840065E+02
           0.1422433E+03    -0.3840048E+02
           0.1422420E+03    -0.3839857E+02
           0.1422436E+03    -0.3839315E+02
      END
      END
      """);
    assertEquals(2, poly.getNumGeometries());
    assertEquals(4.60252e-4, poly.getArea(), 1e-10);
  }

  @Test
  void testParseAustraliaOceana() throws IOException {
    var poly = parsePolyFile("""
      australia-oceania
      1
         -107.863281   11.780702
         -104.171875   -28.082042
         -179.999999   -45.652740
         -179.999999   4.082818
         -107.863281   11.780702
      END
      0
         89.512500   -11.143360
         61.663780   -9.177713
         44.655470   -57.087780
         180.000000   -57.164820
         180.000000   26.277810
         141.547997   22.628320
         130.145100   3.640314
         129.953200   -0.535293
         131.061600   -3.784815
         130.266900   -10.043780
         118.255700   -13.011650
         102.800900   -8.390453
         89.512500   -11.143360
      END
      END
      """);
    assertEquals(2, poly.getNumGeometries());
    assertEquals(10876.51613, poly.getArea(), 1e-4);
  }

  @Test
  void testParseInvalid() throws IOException {
    assertThrows(FileFormatException.class, () -> parsePolyFile("""
      name
      section
         1 2
         3 4
         5 6
         7 8
      """));
    assertThrows(FileFormatException.class, () -> parsePolyFile("""
      name
      section
         1 2
         3 4
         5 6
         7 8
      END
      """));
    parsePolyFile("""
      name
      section
         1 2
         3 4
         5 6
         7 8
      END
      END
      """);
    parsePolyFile("""


      name

      section

         1 2
         3 4
         5 6
         7 8

      END

      END


      """);
    assertThrows(FileFormatException.class, () -> parsePolyFile("""
      name
      section
         1 2
         3 4
         5 6
         7 8
      END
      END
      name
      section
         1 2
         3 4
         5 6
         7 8
      END
      END
      """));
  }

  @Test
  void testParseHole() throws IOException {
    var poly = parsePolyFile("""
      poly
      outer
         0 0
         0 10
         10 10
         10 0
         0 0
      END
      !inner
         1 1
         1 9
         9 9
         9 1
         1 1
      END
      END
      """);
    assertEquals(1, poly.getNumGeometries());
    assertEquals(10 * 10 - 8 * 8, poly.getArea(), 1e-4);
  }
}
