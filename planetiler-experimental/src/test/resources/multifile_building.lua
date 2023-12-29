-- handles building features from the multifile.lua example
local mod = {}

-- multifile.lua builds an optimized MultiExpression matcher from each layer's filter
-- TODO nicer way to build these?
mod.filter = Expression:AND(
  Expression:OR(
    Expression:match_field('building'),
    Expression:match_any('aeroway', 'building', 'hangar')
  ),
  Expression:NOT(Expression:OR(
    Expression:match_any('building', 'no', 'none')
  ))
)
-- when filter matches, this function gets run
function mod.process_feature(source, features, keys)
  features:polygon("building")
      :set_attr('class', keys[1])
      :set_min_zoom(14)
end

return mod
