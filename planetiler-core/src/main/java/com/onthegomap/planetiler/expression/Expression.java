package com.onthegomap.planetiler.expression;

import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.util.Format;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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

  String LINESTRING_TYPE = "linestring";
  String POINT_TYPE = "point";
  String POLYGON_TYPE = "polygon";
  String RELATION_MEMBER_TYPE = "relation_member";

  Set<String> supportedTypes = Set.of(LINESTRING_TYPE, POINT_TYPE, POLYGON_TYPE, RELATION_MEMBER_TYPE);
  Expression TRUE = new Expression() {
    @Override
    public String toString() {
      return "TRUE";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      return true;
    }
  };
  Expression FALSE = new Expression() {
    public String toString() {
      return "FALSE";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      return false;
    }
  };

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
  static MatchAny matchAny(String field, String... values) {
    return matchAny(field, List.of(values));
  }

  /**
   * Returns an expression that evaluates to true if the value for {@code field} tag is any of {@code values}.
   * <p>
   * {@code values} can contain exact matches, "%text%" to match any value containing "text", or "" to match any value.
   */
  static MatchAny matchAny(String field, List<String> values) {
    return new MatchAny(field, values);
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

  private static String listToString(List<?> items) {
    return items.stream().map(Object::toString).collect(Collectors.joining(", "));
  }

  private static Expression simplify(Expression initial) {
    // iteratively simplify the expression until we reach a fixed point and start seeing
    // an expression that's already been seen before
    Expression simplified = initial;
    Set<Expression> seen = new HashSet<>();
    seen.add(simplified);
    while (true) {
      simplified = simplifyOnce(simplified);
      if (seen.contains(simplified) || seen.size() > 100) {
        return simplified;
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
        .filter(child -> child != FALSE)
        .map(Expression::simplifyOnce).toList());
    } else if (expression instanceof And and) {
      if (and.children.isEmpty()) {
        return FALSE;
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
        .filter(child -> child != TRUE)
        .map(Expression::simplifyOnce).toList());
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
  boolean evaluate(SourceFeature input, List<String> matchKeys);

  record And(List<Expression> children) implements Expression {

    @Override
    public String toString() {
      return "and(" + listToString(children) + ")";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
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
    public String toString() {
      return "or(" + listToString(children) + ")";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      int size = children.size();
      // Optimization: this method consumes the most time when matching against input elements, and
      // iterating through this list by index is slightly faster than an enhanced for loop
      // noinspection ForLoopReplaceableByForEach - for intellij
      for (int i = 0; i < size; i++) {
        Expression child = children.get(i);
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
    public String toString() {
      return "not(" + child + ")";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      return !child.evaluate(input, new ArrayList<>());
    }
  }

  /**
   * Evaluates to true if the value for {@code field} tag is any of {@code exactMatches} or contains any of {@code
   * wildcards}.
   *
   * @param values           all raw string values that were initially provided
   * @param exactMatches     the input {@code values} that should be treated as exact matches
   * @param wildcards        the input {@code values} that should be treated as wildcards
   * @param matchWhenMissing if {@code values} contained ""
   */
  record MatchAny(
    String field, List<String> values, Set<String> exactMatches, List<String> wildcards, boolean matchWhenMissing
  ) implements Expression {

    private static final Pattern containsPattern = Pattern.compile("^%(.*)%$");

    MatchAny(String field, List<String> values) {
      this(field, values,
        values.stream().filter(v -> !v.contains("%")).collect(Collectors.toSet()),
        values.stream().filter(v -> v.contains("%")).map(val -> {
          var matcher = containsPattern.matcher(val);
          if (!matcher.matches()) {
            throw new IllegalArgumentException("wildcards must start/end with %: " + val);
          }
          return matcher.group(1);
        }).toList(),
        values.contains("")
      );
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      Object value = input.getTag(field);
      if (value == null) {
        return matchWhenMissing;
      } else {
        String str = value.toString();
        if (exactMatches.contains(str)) {
          matchKeys.add(field);
          return true;
        }
        for (String target : wildcards) {
          if (str.contains(target)) {
            matchKeys.add(field);
            return true;
          }
        }
        return false;
      }
    }

    @Override
    public String toString() {
      return "matchAny(" + Format.quote(field) + ", " + values.stream().map(Format::quote)
        .collect(Collectors.joining(", ")) + ")";
    }
  }

  /** Evaluates to true if an input element contains any value for {@code field} tag. */
  record MatchField(String field) implements Expression {

    @Override
    public String toString() {
      return "matchField(" + Format.quote(field) + ")";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      if (input.hasTag(field)) {
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
    public String toString() {
      return "matchType(" + Format.quote(type) + ")";
    }

    @Override
    public boolean evaluate(SourceFeature input, List<String> matchKeys) {
      return switch (type) {
        case LINESTRING_TYPE -> input.canBeLine();
        case POLYGON_TYPE -> input.canBePolygon();
        case POINT_TYPE -> input.isPoint();
        case RELATION_MEMBER_TYPE -> input.hasRelationInfo();
        default -> false;
      };
    }
  }
}
