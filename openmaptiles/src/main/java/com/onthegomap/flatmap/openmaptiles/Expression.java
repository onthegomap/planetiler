package com.onthegomap.flatmap.openmaptiles;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Expression {

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

  static MatchAny matchAny(String field, String... values) {
    return matchAny(field, List.of(values));
  }

  static MatchAny matchAny(String field, List<String> values) {
    return new MatchAny(field, values);
  }

  static MatchField matchField(String field) {
    return new MatchField(field);
  }

  Set<String> supportedTypes = Set.of("linestring", "point", "polygon", "relation_member");

  static MatchType matchType(String type) {
    if (!supportedTypes.contains(type)) {
      throw new IllegalArgumentException("Unsupported type: " + type);
    }
    return new MatchType(type);
  }

  private static String listToString(List<?> items) {
    return items.stream().map(Object::toString).collect(Collectors.joining(", "));
  }

  default Expression simplify() {
    return simplify(this);
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

  private static Expression simplify(Expression initial) {
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

  default Expression replace(Expression a, Expression b) {
    return replace(a::equals, b);
  }

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

  record And(List<Expression> children) implements Expression {

    @Override
    public String toString() {
      return "and(" + listToString(children) + ")";
    }
  }

  record Or(List<Expression> children) implements Expression {

    @Override
    public String toString() {
      return "or(" + listToString(children) + ")";
    }
  }

  record Not(Expression child) implements Expression {

    @Override
    public String toString() {
      return "not(" + child + ")";
    }
  }

  Expression TRUE = new Expression() {
    public String toString() {
      return "TRUE";
    }
  };

  Expression FALSE = new Expression() {
    public String toString() {
      return "FALSE";
    }
  };

  record MatchAny(String field, List<String> values, Set<String> exactMatches, List<String> wildcards) implements
    Expression {

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
        }).toList()
      );
    }

    @Override
    public String toString() {
      return "matchAny(" + Generate.quote(field) + ", " + values.stream().map(Generate::quote)
        .collect(Collectors.joining(", ")) + ")";
    }
  }

  record MatchField(String field) implements Expression {

    @Override
    public String toString() {
      return "matchField(" + Generate.quote(field) + ")";
    }
  }

  record MatchType(String type) implements Expression {

    @Override
    public String toString() {
      return "matchType(" + Generate.quote(type) + ")";
    }
  }
}
