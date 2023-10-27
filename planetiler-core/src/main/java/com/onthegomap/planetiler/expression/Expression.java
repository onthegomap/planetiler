package com.onthegomap.planetiler.expression;

import static com.onthegomap.planetiler.expression.DataType.GET_TAG;

import com.onthegomap.planetiler.reader.WithGeometryType;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Format;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A framework for defining and manipulating boolean expressions that match on input element.
 * <p>
 * Calling {@code toString()} on any expression will generate code that can be used to recreate an identical copy of the
 * original expression, assuming that the generated code includes:
 * {@snippet :
 * import static com.onthegomap.planetiler.expression.Expression.*;
 * }
 */
// TODO rename to BooleanExpression
public interface Expression extends Simplifiable<Expression> {
  Logger LOGGER = LoggerFactory.getLogger(Expression.class);

  String LINESTRING_TYPE = "linestring";
  String POINT_TYPE = "point";
  String POLYGON_TYPE = "polygon";
  String UNKNOWN_GEOMETRY_TYPE = "unknown_type";

  Set<String> supportedTypes = Set.of(LINESTRING_TYPE, POINT_TYPE, POLYGON_TYPE, UNKNOWN_GEOMETRY_TYPE);
  Expression TRUE = new Constant(true, "TRUE");
  Expression FALSE = new Constant(false, "FALSE");

  List<String> dummyList = new NoopList<>();

  static And and(Expression... children) {
    return and(List.of(children));
  }

  static And and(List<Expression> children) {
    return new And(children);
  }

  static Or or(Expression... children) {
    return or(List.of(children));
  }

  static Or or(List<Expression> children) {
    return new Or(children);
  }

  static Not not(Expression child) {
    return new Not(child);
  }

  /**
   * Returns an expression that evaluates to true if the value for {@code field} tag is any of {@code values}.
   * <p>
   * {@code values} can contain exact matches, "%text%" to match any value containing "text", or "" to match any value.
   */
  static MatchAny matchAny(String field, Object... values) {
    return matchAny(field, List.of(values));
  }

  /**
   * Returns an expression that evaluates to true if the value for {@code field} tag is any of {@code values}.
   * <p>
   * {@code values} can contain exact matches, "%text%" to match any value containing "text", or "" to match any value.
   */
  static MatchAny matchAny(String field, List<?> values) {
    return MatchAny.from(field, GET_TAG, values);
  }

  /**
   * Returns an expression that evaluates to true if the value for {@code field} tag is any of {@code values}, when
   * considering the tag as a specified data type and then converted to a string.
   * <p>
   * {@code values} can contain exact matches, "%text%" to match any value containing "text", or "" to match any value.
   */
  static MatchAny matchAnyTyped(String field, BiFunction<WithTags, String, Object> typeGetter, Object... values) {
    return matchAnyTyped(field, typeGetter, List.of(values));
  }

  /**
   * Returns an expression that evaluates to true if the value for {@code field} tag is any of {@code values}, when
   * considering the tag as a specified data type and then converted to a string.
   * <p>
   * {@code values} can contain exact matches, "%text%" to match any value containing "text", or "" to match any value.
   */
  static MatchAny matchAnyTyped(String field, BiFunction<WithTags, String, Object> typeGetter,
    List<?> values) {
    return MatchAny.from(field, typeGetter, values);
  }

  /** Returns an expression that evaluates to true if the element has any value for tag {@code field}. */
  static MatchField matchField(String field) {
    return new MatchField(field);
  }

  /**
   * Returns an expression that evaluates to true if the geometry of an element matches {@code type}.
   * <p>
   * Allowed values:
   * <ul>
   * <li>"linestring"</li>
   * <li>"point"</li>
   * <li>"polygon"</li>
   * </ul>
   */
  static MatchType matchType(String type) {
    if (!supportedTypes.contains(type)) {
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
    return new MatchType(type);
  }

  private static String generateJavaCodeList(List<Expression> items) {
    return items.stream().map(Expression::generateJavaCode).collect(Collectors.joining(", "));
  }

  /** Returns a copy of this expression where every nested instance of {@code a} is replaced with {@code b}. */
  default Expression replace(Expression a, Expression b) {
    return replace(a::equals, b);
  }

  /**
   * Returns a copy of this expression where every nested instance matching {@code replace} is replaced with {@code b}.
   */
  default Expression replace(Predicate<Expression> replace, Expression b) {
    if (replace.test(this)) {
      return b;
    } else {
      return switch (this) {
        case Not(var child) -> new Not(child.replace(replace, b));
        case Or(var children) -> new Or(children.stream().map(child -> child.replace(replace, b)).toList());
        case And(var children) -> new And(children.stream().map(child -> child.replace(replace, b)).toList());
        default -> this;
      };
    }
  }

  /** Returns true if this expression or any subexpression matches {@code filter}. */
  default boolean contains(Predicate<Expression> filter) {
    if (filter.test(this)) {
      return true;
    } else {
      return switch (this) {
        case Not(var child) -> child.contains(filter);
        case Or(var children) -> children.stream().anyMatch(child -> child.contains(filter));
        case And(var children) -> children.stream().anyMatch(child -> child.contains(filter));
        default -> false;
      };
    }
  }

  /**
   * Returns true if this expression matches an input element.
   *
   * @param input     the input element
   * @param matchKeys list that this method call will add any key to that was responsible for triggering the match
   * @return true if this expression matches the input element
   */
  boolean evaluate(WithTags input, List<String> matchKeys);

  //A list that silently drops all additions
  class NoopList<T> extends ArrayList<T> {
    @Override
    public boolean add(T t) {
      return true;
    }
  }

  /**
   * Returns true if this expression matches an input element.
   *
   * @param input the input element
   * @return true if this expression matches the input element
   */
  default boolean evaluate(WithTags input) {
    return evaluate(input, dummyList);
  }

  /** Returns Java code that can be used to reconstruct this expression. */
  String generateJavaCode();

  /** A constant boolean value. */
  record Constant(boolean value, @Override String generateJavaCode) implements Expression {
    @Override
    public String toString() {
      return generateJavaCode;
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      return value;
    }
  }

  record And(List<Expression> children) implements Expression {

    @Override
    public String generateJavaCode() {
      return "and(" + generateJavaCodeList(children) + ")";
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      for (Expression child : children) {
        if (!child.evaluate(input, matchKeys)) {
          matchKeys.clear();
          return false;
        }
      }
      return true;
    }

    @Override
    public Expression simplifyOnce() {
      if (children.isEmpty()) {
        return TRUE;
      }
      if (children.size() == 1) {
        return children.get(0).simplifyOnce();
      }
      if (children.contains(FALSE)) {
        return FALSE;
      }
      return and(children.stream()
        // hoist children
        .flatMap(child -> child instanceof And childAnd ? childAnd.children.stream() : Stream.of(child))
        .filter(child -> child != TRUE) // and() == and(TRUE) == and(TRUE, TRUE) == TRUE, so safe to remove all here
        .distinct()
        .map(Simplifiable::simplifyOnce).toList());
    }
  }

  record Or(List<Expression> children) implements Expression {

    @Override
    public String generateJavaCode() {
      return "or(" + generateJavaCodeList(children) + ")";
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      for (Expression child : children) {
        if (child.evaluate(input, matchKeys)) {
          return true;
        }
      }
      return false;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == this) {
        return true;
      }
      if (obj == null || obj.getClass() != this.getClass()) {
        return false;
      }
      var that = (Or) obj;
      return Objects.equals(this.children, that.children);
    }

    @Override
    public int hashCode() {
      return Objects.hash(children);
    }

    @Override
    public Expression simplifyOnce() {
      if (children.isEmpty()) {
        return FALSE;
      }
      if (children.size() == 1) {
        return children.get(0).simplifyOnce();
      }
      if (children.contains(TRUE)) {
        return TRUE;
      }
      return or(children.stream()
        // hoist children
        .flatMap(child -> child instanceof Or childOr ? childOr.children.stream() : Stream.of(child))
        .filter(child -> child != FALSE) // or() == or(FALSE) == or(FALSE, FALSE) == FALSE, so safe to remove all here
        .distinct()
        .map(Simplifiable::simplifyOnce).toList());
    }
  }

  record Not(Expression child) implements Expression {

    @Override
    public String generateJavaCode() {
      return "not(" + child.generateJavaCode() + ")";
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      return !child.evaluate(input, new ArrayList<>());
    }

    @Override
    public Expression simplifyOnce() {
      if (child instanceof Or or) {
        return and(or.children.stream().<Expression>map(Expression::not).toList());
      } else if (child instanceof And and) {
        return or(and.children.stream().<Expression>map(Expression::not).toList());
      } else if (child instanceof Not not2) {
        return not2.child;
      } else if (child == TRUE) {
        return FALSE;
      } else if (child == FALSE) {
        return TRUE;
      } else if (child instanceof MatchAny any && any.values.equals(List.of(""))) {
        return matchField(any.field);
      }
      return this;
    }
  }

  /**
   * Evaluates to true if the value for {@code field} tag is any of {@code exactMatches} or contains any of {@code
   * wildcards}.
   *
   * @param values           all raw string values that were initially provided
   * @param exactMatches     the input {@code values} that should be treated as exact matches
   * @param pattern          regular expression that the value must match, or null
   * @param matchWhenMissing if {@code values} contained ""
   */
  record MatchAny(
    String field, List<?> values, Set<String> exactMatches,
    Pattern pattern,
    boolean matchWhenMissing,
    BiFunction<WithTags, String, Object> valueGetter
  ) implements Expression {

    static MatchAny from(String field, BiFunction<WithTags, String, Object> valueGetter, List<?> values) {
      List<String> exactMatches = new ArrayList<>();
      List<String> patterns = new ArrayList<>();

      for (var value : values) {
        if (value != null) {
          String string = value.toString();
          if (string.matches("^.*(?<!\\\\)%.*$")) {
            patterns.add(wildcardToRegex(string));
          } else {
            exactMatches.add(unescape(string));
          }
        }
      }
      boolean matchWhenMissing = values.stream().anyMatch(v -> v == null || "".equals(v));

      return new MatchAny(field, values,
        Set.copyOf(exactMatches),
        patterns.isEmpty() ? null : Pattern.compile("(" + Strings.join(patterns, '|') + ")"),
        matchWhenMissing,
        valueGetter
      );
    }

    private static String wildcardToRegex(String string) {
      StringBuilder regex = new StringBuilder("^");
      StringBuilder token = new StringBuilder();
      while (!string.isEmpty()) {
        if (string.startsWith("\\%")) {
          if (!token.isEmpty()) {
            regex.append(Pattern.quote(token.toString()));
          }
          token.setLength(0);
          regex.append("%");
          string = string.replaceFirst("^\\\\%", "");
        } else if (string.startsWith("%")) {
          if (!token.isEmpty()) {
            regex.append(Pattern.quote(token.toString()));
          }
          token.setLength(0);
          regex.append(".*");
          string = string.replaceFirst("^%+", "");
        } else {
          token.append(string.charAt(0));
          string = string.substring(1);
        }
      }
      if (!token.isEmpty()) {
        regex.append(Pattern.quote(token.toString()));
      }
      regex.append('$');
      return regex.toString();
    }

    private static String unescape(String input) {
      return input.replace("\\%", "%");
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      Object value = valueGetter.apply(input, field);
      if (value == null || "".equals(value)) {
        return matchWhenMissing;
      } else {
        String str = value.toString();
        if (exactMatches.contains(str)) {
          matchKeys.add(field);
          return true;
        }
        if (pattern != null && pattern.matcher(str).matches()) {
          matchKeys.add(field);
          return true;
        }
        return false;
      }
    }

    @Override
    public Expression simplifyOnce() {
      return isMatchAnything() ? matchField(field) : this;
    }

    @Override
    public String generateJavaCode() {
      // java code generation only needed for the simple cases used by openmaptiles schema generation
      List<String> valueStrings = new ArrayList<>();

      if (GET_TAG != valueGetter) {
        throw new UnsupportedOperationException("Code generation only supported for default getTag");
      }

      for (var value : values) {
        if (value instanceof String string) {
          valueStrings.add(Format.quote(string));
        } else {
          throw new UnsupportedOperationException("Code generation only supported for string values, found: " +
            value.getClass().getCanonicalName() + " " + value);
        }
      }
      return "matchAny(" + Format.quote(field) + ", " + String.join(", ", valueStrings) + ")";
    }

    public boolean isMatchAnything() {
      return !matchWhenMissing && exactMatches.isEmpty() && (pattern != null && pattern.toString().equals("(^.*$)"));
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof MatchAny matchAny &&
        matchWhenMissing == matchAny.matchWhenMissing &&
        Objects.equals(field, matchAny.field) &&
        Objects.equals(values, matchAny.values) &&
        Objects.equals(exactMatches, matchAny.exactMatches) &&
        // Patterns for the same input string are not equal
        Objects.equals(patternString(), matchAny.patternString()) &&
        Objects.equals(valueGetter, matchAny.valueGetter));
    }

    private String patternString() {
      return pattern == null ? null : pattern.pattern();
    }

    @Override
    public int hashCode() {
      return Objects.hash(field, values, exactMatches, patternString(), matchWhenMissing, valueGetter);
    }
  }

  /** Evaluates to true if an input element contains any value for {@code field} tag. */
  record MatchField(String field) implements Expression {

    @Override
    public String generateJavaCode() {
      return "matchField(" + Format.quote(field) + ")";
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      Object value = input.getTag(field);
      if (value != null && !"".equals(value)) {
        matchKeys.add(field);
        return true;
      }
      return false;
    }
  }

  /**
   * Evaluates to true if an input element has geometry type matching {@code type}.
   */
  record MatchType(String type) implements Expression {

    @Override
    public String generateJavaCode() {
      return "matchType(" + Format.quote(type) + ")";
    }

    @Override
    public boolean evaluate(WithTags input, List<String> matchKeys) {
      if (input instanceof WithGeometryType withGeom) {
        return switch (type) {
          case LINESTRING_TYPE -> withGeom.canBeLine();
          case POLYGON_TYPE -> withGeom.canBePolygon();
          case POINT_TYPE -> withGeom.isPoint();
          default -> false;
        };
      } else {
        return false;
      }
    }
  }
}
