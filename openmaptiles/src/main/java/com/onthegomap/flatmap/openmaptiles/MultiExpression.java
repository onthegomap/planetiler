package com.onthegomap.flatmap.openmaptiles;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public record MultiExpression<T>(Map<T, Expression> expressions) {

  public static <T> MultiExpression<T> of(Map<T, Expression> expressions) {
    return new MultiExpression<>(expressions);
  }

  public MultiExpressionIndex<T> index() {
    return new MultiExpressionIndex<>(this);
  }

  public static class MultiExpressionIndex<T> {

    private static final AtomicInteger ids = new AtomicInteger(0);
    // index from source feature tag key to the expressions that include it so that
    // we can limit the number of expressions we need to evaluate for each input
    // and improve matching performance by ~5x
    private final Map<String, List<ExpressionValue<T>>> keyToExpressionsMap;
    // same thing as a list (optimized for iteration when # source feature keys > # tags we care about)
    private final List<Map.Entry<String, List<ExpressionValue<T>>>> keyToExpressionsList;

    private MultiExpressionIndex(MultiExpression<T> expressions) {
      Map<String, Set<ExpressionValue<T>>> keyToExpressions = new HashMap<>();
      for (var entry : expressions.expressions.entrySet()) {
        T result = entry.getKey();
        Expression exp = entry.getValue();
        ExpressionValue<T> expressionValue = new ExpressionValue<>(exp, result);
        getRelevantKeys(exp, key -> keyToExpressions.computeIfAbsent(key, k -> new HashSet<>()).add(expressionValue));
      }
      keyToExpressionsMap = new HashMap<>();
      keyToExpressions.forEach((key, value) -> keyToExpressionsMap.put(key, value.stream().toList()));
      keyToExpressionsList = keyToExpressionsMap.entrySet().stream().toList();
    }

    private static void getRelevantKeys(Expression exp, Consumer<String> acceptKey) {
      if (exp instanceof Expression.And and) {
        and.children().forEach(child -> getRelevantKeys(child, acceptKey));
      } else if (exp instanceof Expression.Or or) {
        or.children().forEach(child -> getRelevantKeys(child, acceptKey));
      } else if (exp instanceof Expression.Not) {
        // ignore anything that's purely used as a filter
      } else if (exp instanceof Expression.MatchField field) {
        acceptKey.accept(field.field());
      } else if (exp instanceof Expression.MatchAny any) {
        acceptKey.accept(any.field());
      }
    }

    private static boolean evaluate(Expression expr, Map<String, Object> input, List<String> matchKeys) {
      // optimization: since this is evaluated for every input element, use
      // simple for loops instead of enhanced to avoid overhead of generating the
      // iterator (~30% speedup)

      if (expr instanceof Expression.MatchAny match) {
        Object value = input.get(match.field());
        if (value == null) {
          return false;
        } else {
          String str = value.toString();
          if (match.exactMatches().contains(str)) {
            matchKeys.add(match.field());
            return true;
          }
          List<String> wildcards = match.wildcards();
          for (int i = 0; i < wildcards.size(); i++) {
            var target = wildcards.get(i);
            if (str.contains(target)) {
              matchKeys.add(match.field());
              return true;
            }
          }
          return false;
        }
      } else if (expr instanceof Expression.MatchField match) {
        matchKeys.add(match.field());
        return input.containsKey(match.field());
      } else if (expr instanceof Expression.Or or) {
        List<Expression> children = or.children();
        for (int i = 0; i < children.size(); i++) {
          Expression child = children.get(i);
          if (evaluate(child, input, matchKeys)) {
            return true;
          }
        }
        return false;
      } else if (expr instanceof Expression.And and) {
        List<Expression> children = and.children();
        for (int i = 0; i < children.size(); i++) {
          Expression child = children.get(i);
          if (!evaluate(child, input, matchKeys)) {
            matchKeys.clear();
            return false;
          }
        }
        return true;
      } else if (expr instanceof Expression.Not not) {
        return !evaluate(not.child(), input, new ArrayList<>());
      } else {
        throw new IllegalArgumentException("Unrecognized expression: " + expr);
      }
    }

    public static record MatchWithTriggers<T>(T match, List<String> keys) {}

    public List<MatchWithTriggers<T>> getMatchesWithTriggers(Map<String, Object> input) {
      List<MatchWithTriggers<T>> result = new ArrayList<>();
      BitSet visited = new BitSet(ids.get());
      if (input.size() < keyToExpressionsMap.size()) {
        for (String inputKey : input.keySet()) {
          var expressionValues = keyToExpressionsMap.get(inputKey);
          visitExpression(input, result, visited, expressionValues);
        }
      } else {
        // optimization: since this is evaluated for every element, generating an iterator
        // for enhanced for loop becomes a bottleneck so use simple for loop over list instead
        for (int i = 0; i < keyToExpressionsList.size(); i++) {
          var entry = keyToExpressionsList.get(i);
          var expressionValues = entry.getValue();
          if (input.containsKey(entry.getKey())) {
            visitExpression(input, result, visited, expressionValues);
          }
        }
      }
      return result;
    }

    public List<T> getMatches(Map<String, Object> input) {
      List<MatchWithTriggers<T>> matches = getMatchesWithTriggers(input);
      return matches.stream().map(d -> d.match).toList();
    }

    public T getOrElse(Map<String, Object> input, T defaultValue) {
      List<T> matches = getMatches(input);
      return matches.isEmpty() ? defaultValue : matches.get(0);
    }

    private void visitExpression(Map<String, Object> input, List<MatchWithTriggers<T>> result, BitSet visited,
      List<ExpressionValue<T>> expressionValues) {
      if (expressionValues != null) {
        // optimization: since this is evaluated for every element, generating an iterator
        // for enhanced for loop becomes a bottleneck so use simple for loop over list instead
        for (int i = 0; i < expressionValues.size(); i++) {
          var expressionValue = expressionValues.get(i);
          if (!visited.get(expressionValue.id)) {
            visited.set(expressionValue.id);
            List<String> matchKeys = new ArrayList<>();
            if (evaluate(expressionValue.exp(), input, matchKeys)) {
              result.add(new MatchWithTriggers<>(expressionValue.result, matchKeys));
            }
          }
        }
      }
    }

    private static record ExpressionValue<T>(Expression exp, T result, int id) {

      ExpressionValue(Expression exp, T result) {
        this(exp, result, ids.getAndIncrement());
      }
    }
  }
}
