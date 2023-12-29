-- example profile that delegates handling for individual layers to separate files
planetiler.examples = "multifile.spec.yaml"

planetiler.add_source('osm', {
  type = 'osm',
  url = 'geofabrik:monaco',
})

local layers = {
  require("multifile_building"),
  require("multifile_housenumber"),
}

-- TODO make a java utility that does this in a more complete, less verbose way
-- (handle other profile methods, separate handler methods per source, expose layer name, etc.)
local processors = {}
for i, layer in ipairs(layers) do
  -- classes defined in LuaEnvironment.CLASSES_TO_EXPOSE are exposed as global variables to the profile
  table.insert(processors, MultiExpression:entry(layer.process_feature, layer.filter))
end
local feature_processors = MultiExpression:of(processors):index()

function planetiler.process_feature(source, features)
  for i, match in ipairs(feature_processors:get_matches_with_triggers(source)) do
    match.match(source, features, match.keys)
  end
end
