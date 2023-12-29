-- Example lua profile that emits power lines and poles from an openstreetmap source
-- useful for hot air ballooning

-- The planetiler object defined in LuaEnvironment.PlanetilerNamespace is the interface for sharing
-- data between lua scripts and Java
planetiler.output.name = "Power"
planetiler.output.description = "Simple"
planetiler.output.attribution =
'<a href="https://www.openstreetmap.org/copyright" target="_blank">&copy;OpenStreetMap contributors</a>'
planetiler.examples = "power.spec.yaml"
planetiler.output.path = { "data", "power.pmtiles" }

local area = planetiler.args:get_string("area", "geofabrik area to download", "massachusetts")

planetiler.add_source('osm', {
  type = 'osm',
  url = 'geofabrik:' .. area,
  -- any java method or field that takes a Path can be called with a list of path parts from lua
  path = { 'data', 'sources', area .. '.osm.pbf' }
})

function planetiler.process_feature(source, features)
  if source:can_be_line() and source:has_tag("power", "line") then
    features
        :line("power")
        :set_min_zoom(7)
        :inherit_attr_from_source("power")
        :inherit_attr_from_source("voltage")
        :inherit_attr_from_source("cables")
        :inherit_attr_from_source("operator")
  elseif source:isPoint() and source:has_tag("power", "pole") then
    features
        :point("power")
        :set_min_zoom(13)
        :inherit_attr_from_source("power")
        :inherit_attr_from_source("ref")
        :inherit_attr_from_source("height")
        :inherit_attr_from_source("operator")
  end
end
