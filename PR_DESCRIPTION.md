## Summary

This PR adds support for a new `geometry: relation_members` option in YAML custom maps that allows processing individual members of OSM relations as separate features. Instead of emitting one feature per relation, this emits one feature per qualifying member, using each member's geometry and tags.

## Problem

Currently, when working with OSM relations in YAML custom maps, you can only emit a single feature for the entire relation. However, many use cases require processing individual members of a relation as separate features:

- **Route relations** (`type=route`): Extract individual road/rail segments that make up a route
- **Boundary relations** (`type=boundary`): Extract individual boundary segments
- **Public transport routes**: Extract individual stops or route segments
- **Waterway relations**: Extract individual river segments

## Solution

Add a new `geometry: relation_members` option that:
1. **Selects relations** using existing `include_when`/`exclude_when` filters (applied to relation tags)
2. **Emits one feature per qualifying member** instead of one feature per relation
3. **Uses each member's geometry** (way → line/polygon, node → point)
4. **Supports member-level filtering** by type, role, and tags
5. **Supports member-level attributes** from member tags

## New Configuration Fields

All fields are optional and only valid with `geometry: relation_members`:

- **`member_types`**: Filter members by OSM element type (`"node"`, `"way"`, `"relation"`)
- **`member_roles`**: Filter members by their role in the relation
- **`member_include_when`**: Filter members by their tags (supports structured expressions and inline scripts)
- **`member_exclude_when`**: Exclude members by their tags (supports structured expressions and inline scripts)
- **`member_attributes`**: Add attributes to each member feature from member tags (supports inline scripts)

## Example Usage

```yaml
layers:
  - id: route_segments
    features:
      - source: osm
        geometry: relation_members
        include_when:
          type: route
          route: railway
        member_types: [way]
        member_roles: [""]
        member_include_when:
          railway: rail
        attributes:
          - key: route_name
            tag_value: name
        member_attributes:
          - key: segment_ref
            tag_value: ref
          - key: combined_name
            value: '${ member.tags.name + " (" + feature.tags.name + ")" }'
```

## Implementation Details

- Follows the multipolygon processing pattern (two-phase approach)
- Phase 1: Identify relations matching `relation_members` features
- Phase 2: Store member geometries/tags, then process relations to emit member features
- New classes: `RelationSourceFeature`, `RelationMemberDataProvider`, `RelationMembersInfo`
- New context: `MemberContext` for member-level expression evaluation
- Comprehensive validation with clear error messages

## Testing

- ✅ Unit tests for validation logic (`RelationMembersValidationTest`)
- ✅ Integration tests for schema loading (`ConfiguredFeatureTest.testInvalidSchemas`)
- ✅ Valid schema examples (`relation_members_basic.yml`)
- ✅ Invalid schema examples (validation error cases)
- ✅ All existing tests pass

## Behavior Notes

- Feature selection (`include_when`/`exclude_when`) applies to the relation, not members
- Member filtering order: type → role → `member_include_when` → `member_exclude_when`
- Duplicate members are skipped (first occurrence processed)
- Missing members are skipped with logged errors
- Nested relations are currently skipped (not recursively processed)
- Post-processing operations like `merge_linestrings` can be used with `relation_members`

## Related

Resolves #1426

