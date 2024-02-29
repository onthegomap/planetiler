package com.onthegomap.planetiler.expression;

import static com.onthegomap.planetiler.expression.Expression.FALSE;
import static com.onthegomap.planetiler.expression.Expression.TRUE;
import static com.onthegomap.planetiler.expression.Expression.matchType;

import com.onthegomap.planetiler.reader.WithGeometryType;
import com.onthegomap.planetiler.reader.WithTags;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A list of {@link Expression Expressions} to evaluate on input elements.
 * <p>
 * {@link #index()} returns an optimized {@link Index} that evaluates the minimal set of expressions on the keys present
 * on the element.
 * <p>
 * {@link Index#getMatches(WithTags)} )} returns the data value associated with the expressions that match an input
 * element.
 *
 * @param <T> type of data value associated with each expression
 */
public record MultiExpression<T> (List<Entry<T>> expressions) implements Simplifiable<MultiExpression<T>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MultiExpression.class);
  private static final Comparator<WithId> BY_ID = Comparator.comparingInt(WithId::id);

  public static <T> MultiExpression<T> of(List<Entry<T>> expressions) {
    return new MultiExpression<>(expressions);
  }

  public static <T> Entry<T> entry(T result, Expression expression) {
    return new Entry<>(result, expression);
  }

  /**
   * Returns true if {@code expression} only contains "not filter" so we can't limit evaluating this expression to only
   * when a particular key is present on the input.
   */
  private static boolean mustAlwaysEvaluate(Expression expression) {
    return switch (expression) {
      case Expression.Or(var children) -> children.stream().anyMatch(MultiExpression::mustAlwaysEvaluate);
      case Expression.And(var children) -> children.stream().allMatch(MultiExpression::mustAlwaysEvaluate);
      case Expression.Not(var child) -> !mustAlwaysEvaluate(child);
      case Expression.MatchAny any when any.matchWhenMissing() -> true;
      case null, default -> !(expression instanceof Expression.MatchAny) &&
          !(expression instanceof Expression.MatchField) &&
          !FALSE.equals(expression);
    };
  }

  /** Calls {@code acceptKey} for every tag that could possibly cause {@code exp} to match an input element. */
  private static void getRelevantKeys(Expression exp, Consumer<String> acceptKey) {
    // if a sub-expression must always be evaluated, then either the whole expression must always be evaluated
    // or there is another part of the expression that limits the elements on which it must be evaluated, so we can
    // ignore keys from this sub-expression.
    if (!mustAlwaysEvaluate(exp)) {
      if (exp instanceof Expression.And and) {
        and.children().forEach(child -> getRelevantKeys(child, acceptKey));
      } else if (exp instanceof Expression.Or or) {
        or.children().forEach(child -> getRelevantKeys(child, acceptKey));
      } else if (exp instanceof Expression.MatchField field) {
        acceptKey.accept(field.field());
      } else if (exp instanceof Expression.MatchAny any && !any.matchWhenMissing()) {
        acceptKey.accept(any.field());
      }
      // ignore not case since not(matchAny("field", "")) should track "field" as a relevant key, but that gets
      // simplified to matchField("field") so don't need to handle that here
    }
  }

  /** Returns an optimized index for matching {@link #expressions()} against each input element. */
  public Index<T> index() {
    return index(false);
  }

  /**
   * Same as {@link #index()} but logs a warning when there are degenerate expressions that must be evaluated on every
   * input.
   */
  public Index<T> indexAndWarn() {
    return index(true);
  }

  private Index<T> index(boolean warn) {
    if (expressions.isEmpty()) {
      return new EmptyIndex<>();
    }
    boolean caresAboutGeometryType =
      expressions.stream().anyMatch(entry -> entry.expression.contains(exp -> exp instanceof Expression.MatchType));
    return caresAboutGeometryType ? new GeometryTypeIndex<>(this, warn) : new KeyIndex<>(simplify(), warn);
  }

  /** Returns a copy of this multi-expression that replaces every expression using {@code mapper}. */
  public MultiExpression<T> map(UnaryOperator<Expression> mapper) {
    return new MultiExpression<>(
      expressions.stream()
        .map(entry -> entry(entry.result, mapper.apply(entry.expression).simplify()))
        .filter(entry -> entry.expression != Expression.FALSE)
        .toList()
    );
  }

  /**
   * Returns a copy of this multi-expression that replaces every sub-expression that matches {@code test} with
   * {@code b}.
   */
  public MultiExpression<T> replace(Predicate<Expression> test, Expression b) {
    return map(e -> e.replace(test, b));
  }

  /**
   * Returns a copy of this multi-expression that replaces every sub-expression equal to {@code a} with {@code b}.
   */
  public MultiExpression<T> replace(Expression a, Expression b) {
    return map(e -> e.replace(a, b));
  }

  /** Returns a copy of this multi-expression with each expression simplified. */
  @Override
  public MultiExpression<T> simplifyOnce() {
    return map(Simplifiable::simplify);
  }

  /** Returns a copy of this multi-expression, filtering-out the entry for each data value matching {@code accept}. */
  public MultiExpression<T> filterResults(Predicate<T> accept) {
    return new MultiExpression<>(
      expressions.stream()
        .filter(entry -> accept.test(entry.result))
        .toList()
    );
  }

  /** Returns a copy of this multi-expression, replacing the data value with {@code fn}. */
  public <U> MultiExpression<U> mapResults(Function<T, U> fn) {
    return new MultiExpression<>(
      expressions.stream()
        .map(entry -> entry(fn.apply(entry.result), entry.expression))
        .filter(entry -> entry.result != null)
        .toList()
    );
  }

  /**
   * An optimized index for finding which expressions match an input element.
   *
   * @param <O> type of data value associated with each expression
   */
  public interface Index<O> {

    List<Match<O>> getMatchesWithTriggers(WithTags input);

    /** Returns all data values associated with expressions that match an input element. */
    default List<O> getMatches(WithTags input) {
      return getMatchesWithTriggers(input).stream().map(d -> d.match).toList();
    }

    /**
     * Returns the data value associated with the first expression that match an input element, or {@code defaultValue}
     * if none match.
     */
    default O getOrElse(WithTags input, O defaultValue) {
      List<O> matches = getMatches(input);
      return matches.isEmpty() ? defaultValue : matches.getFirst();
    }

    /**
     * Returns the data value associated with expressions matching a feature with {@code tags}.
     */
    default O getOrElse(Map<String, Object> tags, O defaultValue) {
      List<O> matches = getMatches(WithTags.from(tags));
      return matches.isEmpty() ? defaultValue : matches.getFirst();
    }

    /** Returns true if any expression matches that tags from an input element. */
    default boolean matches(WithTags input) {
      return !getMatchesWithTriggers(input).isEmpty();
    }

    default boolean isEmpty() {
      return false;
    }
  }

  private interface WithId {

    int id();
  }

  private static class EmptyIndex<T> implements Index<T> {

    @Override
    public List<Match<T>> getMatchesWithTriggers(WithTags input) {
      return List.of();
    }

    @Override
    public boolean isEmpty() {
      return true;
    }
  }

  /** Index that limits the search space of expressions based on keys present on an input element. */
  private static class KeyIndex<T> implements Index<T> {

    private final int numExpressions;
    // index from source feature tag key to the expressions that include it so that
    // we can limit the number of expressions we need to evaluate for each input,
    // improves matching performance by ~5x
    private final Map<String, List<EntryWithId<T>>> keyToExpressionsMap;
    // same as keyToExpressionsMap but as a list (optimized for iteration when # source feature keys > # tags we care about)
    private final List<Map.Entry<String, List<EntryWithId<T>>>> keyToExpressionsList;
    // expressions that must always be evaluated on each input element
    private final List<EntryWithId<T>> alwaysEvaluateExpressionList;

    private KeyIndex(MultiExpression<T> expressions, boolean warn) {
      int id = 1;
      // build the indexes
      Map<String, Set<EntryWithId<T>>> keyToExpressions = new HashMap<>();
      List<EntryWithId<T>> always = new ArrayList<>();

      for (var entry : expressions.expressions) {
        Expression expression = entry.expression;
        EntryWithId<T> expressionValue = new EntryWithId<>(entry.result, expression, id++);
        if (mustAlwaysEvaluate(expression)) {
          always.add(expressionValue);
        } else {
          getRelevantKeys(expression,
            key -> keyToExpressions.computeIfAbsent(key, k -> new HashSet<>()).add(expressionValue));
        }
      }
      // create immutable copies for fast iteration at matching time
      if (warn && !always.isEmpty()) {
        LOGGER.warn("{} expressions will be evaluated for every element:", always.size());
        for (var expression : always) {
          LOGGER.warn("    {}: {}", expression.result, expression.expression);
        }
      }
      alwaysEvaluateExpressionList = List.copyOf(always);
      keyToExpressionsMap = keyToExpressions.entrySet().stream().collect(Collectors.toUnmodifiableMap(
        Map.Entry::getKey,
        entry -> entry.getValue().stream().toList()
      ));
      keyToExpressionsList = List.copyOf(keyToExpressionsMap.entrySet());
      numExpressions = id;
    }

    /**
     * Evaluates a list of expressions on an input element, storing the matches into {@code result} and using
     * {@code visited} to avoid evaluating an expression more than once.
     */
    private static <T> void visitExpressions(WithTags input, List<Match<T>> result,
      boolean[] visited, List<EntryWithId<T>> expressions) {
      if (expressions != null) {
        for (EntryWithId<T> expressionValue : expressions) {
          if (!visited[expressionValue.id]) {
            visited[expressionValue.id] = true;
            List<String> matchKeys = new ArrayList<>();
            if (expressionValue.expression().evaluate(input, matchKeys)) {
              result.add(new Match<>(expressionValue.result, matchKeys, expressionValue.id));
            }
          }
        }
      }
    }

    /** Lookup matches in this index for expressions that match a certain type. */
    @Override
    public List<Match<T>> getMatchesWithTriggers(WithTags input) {
      List<Match<T>> result = new ArrayList<>();
      boolean[] visited = new boolean[numExpressions];
      visitExpressions(input, result, visited, alwaysEvaluateExpressionList);
      Map<String, Object> tags = input.tags();
      if (tags.size() < keyToExpressionsMap.size()) {
        for (String inputKey : tags.keySet()) {
          visitExpressions(input, result, visited, keyToExpressionsMap.get(inputKey));
        }
      } else {
        for (var entry : keyToExpressionsList) {
          if (tags.containsKey(entry.getKey())) {
            visitExpressions(input, result, visited, entry.getValue());
          }
        }
      }
      result.sort(BY_ID);
      return result;
    }
  }

  /** Index that limits the search space of expressions based on geometry type of an input element. */
  private static class GeometryTypeIndex<T> implements Index<T> {

    private final KeyIndex<T> pointIndex;
    private final KeyIndex<T> lineIndex;
    private final KeyIndex<T> polygonIndex;
    private final KeyIndex<T> otherIndex;

    private GeometryTypeIndex(MultiExpression<T> expressions, boolean warn) {
      // build an index per type then search in each of those indexes based on the geometry type of each input element
      // this narrows the search space substantially, improving matching performance
      pointIndex = indexForType(expressions, Expression.POINT_TYPE, warn);
      lineIndex = indexForType(expressions, Expression.LINESTRING_TYPE, warn);
      polygonIndex = indexForType(expressions, Expression.POLYGON_TYPE, warn);
      otherIndex = indexForType(expressions, Expression.UNKNOWN_GEOMETRY_TYPE, warn);
    }

    private KeyIndex<T> indexForType(MultiExpression<T> expressions, String type, boolean warn) {
      return new KeyIndex<>(
        expressions
          .replace(matchType(type), TRUE)
          .replace(e -> e instanceof Expression.MatchType, FALSE)
          .simplify(),
        warn
      );
    }

    /**
     * Returns all data values associated with expressions that match an input element, along with the tag keys that
     * caused the match.
     */
    public List<Match<T>> getMatchesWithTriggers(WithTags input) {
      List<Match<T>> result;
      if (input instanceof WithGeometryType withGeometryType) {
        if (withGeometryType.isPoint()) {
          result = pointIndex.getMatchesWithTriggers(input);
        } else if (withGeometryType.canBeLine()) {
          result = lineIndex.getMatchesWithTriggers(input);
          // closed ways can be lines or polygons, unless area=yes or no
          if (withGeometryType.canBePolygon()) {
            result.addAll(polygonIndex.getMatchesWithTriggers(input));
          }
        } else if (withGeometryType.canBePolygon()) {
          result = polygonIndex.getMatchesWithTriggers(input);
        } else {
          result = otherIndex.getMatchesWithTriggers(input);
        }
      } else {
        result = otherIndex.getMatchesWithTriggers(input);
      }
      result.sort(BY_ID);
      return result;
    }
  }

  /** An expression/value pair with unique ID to store whether we evaluated it yet. */
  private record EntryWithId<T> (T result, Expression expression, @Override int id) implements WithId {}

  /**
   * An {@code expression} to evaluate on input elements and {@code result} value to return when the element matches.
   */
  public record Entry<T> (T result, Expression expression) {}

  /** The result when an expression matches, along with the input element tag {@code keys} that triggered the match. */
  public record Match<T> (T match, List<String> keys, @Override int id) implements WithId {}
}
