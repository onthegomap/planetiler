package org.apache.parquet.filter2.predicate;

import java.util.List;
import org.apache.parquet.hadoop.metadata.ColumnPath;

/**
 * Create {@link Operators.DoubleColumn} and {@link Operators.FloatColumn} instances with dots in the column names since
 * their constructors are package-private.
 */
public class Filters {
  private Filters() {}

  public static Operators.DoubleColumn doubleColumn(List<String> parts) {
    return new Operators.DoubleColumn(ColumnPath.get(parts.toArray(String[]::new)));
  }

  public static Operators.FloatColumn floatColumn(List<String> parts) {
    return new Operators.FloatColumn(ColumnPath.get(parts.toArray(String[]::new)));
  }
}
