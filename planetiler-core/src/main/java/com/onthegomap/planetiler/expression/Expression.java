package com.onthegomap.planetiler.expression;

import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.WithTags;
import com.onthegomap.planetiler.util.Format;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A framework for defining and manipulating boolean expressions that match on input element.
 * <p>
 * Calling {@code toString()} on any expression will generate code that can be used to recreate an identical copy of the
 * original expression, assuming that the generated code includes:
 *
 * <pre>
 * {@code
 * import static com.onthegomap.planetiler.expression.Expression.*;
 * }
 * </pre>
 */
public interface Expression {
  Logger LOGGER = LoggerFactory.getLogger(Expression.class);

  String LINESTRING_TYPE = "linestring";
  String POINT_TYPE = "point";
  String POLYGON_TYPE = "polygon";
  String RELATION_MEMBER_TYPE = "relation_member";

  Set<String> supportedTypes = Set.of(LINESTRING_TYPE, POINT_TYPE, POLYGON_TYPE, RELATION_MEMBER_TYPE);
  Expression TRUE = new Constant(true, "TRUE");
  Expression FALSE = new Constant(false, "FALSE");
  BiFunction<WithTags, String, Object> GET_TAG = WithTags::getTag;

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
   * <li>"relation_member"</li>
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

  private static Expression simplify(Expression initial) {
    // iteratively simplify the expression until we reach a fixed point and start seeing
    // an expression that's already been seen before
    Expression simplified = initial;
    Set<Expression> seen = new HashSet<>();
    seen.add(simplified);
    while (true) {
      simplified = simplifyOnce(simplified);
      if (seen.contains(simplified)) {
        return simplified;
      }
      if (seen.size() > 1000) {
        throw new IllegalStateException("Infinite loop while simplifying expression " + initial);
      }
      seen.add(simplified);
    }
  }

  private static Expression simplifyOnce(Expression expression) {
    if (expression instanceof Not not) {
      if (not.child instanceof Or or) {
        return and(or.children.stream().<Expression>map(Expression::not).toList());
      } else if (not.child instanceof And and) {
        return or(and.children.stream().<Expression>map(Expression::not).toList());
      } else if (not.child instanceof Not not2) {
        return not2.child;
      } else if (not.child == TRUE) {
        return FALSE;
      } else if (not.child == FALSE) {
        return TRUE;
      } else if (not.child instanceof MatchAny any && any.values.equals(List.of(""))) {
        return matchField(any.field);
      }
      return not;
    } else if (expression instanceof Or or) {
      if (or.children.isEmpty()) {
        return FALSE;
      }
      if (or.children.size() == 1) {
        return simplifyOnce(or.children.get(0));
      }
      if (or.children.contains(TRUE)) {
        return TRUE;
      }
      return or(or.children.stream()
        // hoist children
        .flatMap(child -> child instanceof Or childOr ? childOr.children.stream() : Stream.of(child))
        .filter(child -> child != FALSE) // or() == or(FALSE) == or(FALSE, FALSE) == FALSE, so safe to remove all here
        .map(Expression::simplifyOnce).toList());
    } else if (expression instanceof And and) {
      if (and.children.isEmpty()) {
        return TRUE;
      }
      if (and.children.size() == 1) {
        return simplifyOnce(and.children.get(0));
      }
      if (and.children.contains(FALSE)) {
        return FALSE;
      }
      return and(and.children.stream()
        // hoist children
        .flatMap(child -> child instanceof And childAnd ? childAnd.children.stream() : Stream.of(child))
        .filter(child -> child != TRUE) // and() == and(TRUE) == and(TRUE, TRUE) == TRUE, so safe to remove all here
        .map(Expression::simplifyOnce).toList());
    } else if (expression instanceof MatchAny any && any.isMatchAnything()) {
      return matchField(any.field);
    } else {
      return expression;
    }
  }

  /** Returns an equivalent, simplified copy of this expression but does not modify {@code this}. */
  default Expression simplify() {
    return simplify(this);
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
    } else if (this instanceof Not not) {
      return new Not(not.child.replace(replace, b));
    } else if (this instanceof Or or) {
      return new Or(or.children.stream().map(child -> child.replace(replace, b)).toList());
    } else if (this instanceof And and) {
      return new And(and.children.stream().map(child -> child.replace(replace, b)).toList());
    } else {
      return this;
    }
  }

  /** Returns true if this expression or any subexpression matches {@code filter}. */
  default boolean contains(Predicate<Expression> filter) {
    if (filter.test(this)) {
      return true;
    } else if (this instanceof Not not) {
      return not.child.contains(filter);
    } else if (this instanceof Or or) {
      return or.children.stream().anyMatch(child -> child.contains(filter));
    } else if (this instanceof And and) {
      return and.children.stream().anyMatch(child -> child.contains(filter));
    } else {
      return false;
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
    private static final long serialVersionUID = 1L;

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
  }

  /**
   * Evaluates to true if the value for {@code field} tag is any of {@code exactMatches} or contains any of {@code
   * wildcards}.
   *
   * @param values           all raw string values that were initially provided
   * @param exactMatches     the input {@code values} that should be treated as exact matches
   * @param patterns         regular expressions that the value must match
   * @param matchWhenMissing if {@code values} contained ""
   */
  record MatchAny(
    String field, List<?> values, Set<String> exactMatches,
    List<Pattern> patterns,
    boolean matchWhenMissing,
    BiFunction<WithTags, String, Object> valueGetter
  ) implements Expression {

    static MatchAny from(String field, BiFunction<WithTags, String, Object> valueGetter, List<?> values) {
      List<String> exactMatches = new ArrayList<>();
      List<Pattern> patterns = new ArrayList<>();

      for (var value : values) {
        if (value != null) {
          String string = value.toString();
          if (string.matches("^.*(?<!\\\\)%.*$")) {
            patterns.add(wildcardToRegex(string));
          } else {
            exactMatches.add(unescape(string.replaceAll("(^%+|(?<!\\\\)%+$)", "")));
          }
        }
      }
      boolean matchWhenMissing = values.stream().anyMatch(v -> v == null || "".equals(v));

      return new MatchAny(field, values,
        Set.copyOf(exactMatches),
        List.copyOf(patterns),
        matchWhenMissing,
        valueGetter
      );
    }

    private static Pattern wildcardToRegex(String string) {
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
      return Pattern.compile(regex.toString());
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
        for (Pattern pattern : patterns) {
          if (pattern.matcher(str).matches()) {
            matchKeys.add(field);
            return true;
          }
        }
        return false;
      }
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
      return !matchWhenMissing && exactMatches.isEmpty() &&
        patterns.stream().allMatch(p -> p.toString().equals("^.*$"));
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
      if (input instanceof SourceFeature sourceFeature) {
        return switch (type) {
          case LINESTRING_TYPE -> sourceFeature.canBeLine();
          case POLYGON_TYPE -> sourceFeature.canBePolygon();
          case POINT_TYPE -> sourceFeature.isPoint();
          case RELATION_MEMBER_TYPE -> sourceFeature.hasRelationInfo();
          default -> false;
        };
      } else {
        return false;
      }
    }
  }
}
