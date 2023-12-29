-- handles addr:housenumber features from the multifile.lua example
local mod = {}

mod.filter = Expression:match_field('addr:housenumber')

function mod.process_feature(source, features)
  features:point("housenumber")
      :set_attr("housenumber", source:get_tag("addr:housenumber"))
      :set_min_zoom(14)
end

return mod
