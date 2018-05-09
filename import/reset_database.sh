#!/bin/bash

PGPASSWORD='%(db_pass)s'
export PGHOST='%(db_host)s'
export PGPORT='%(db_port)d'
export PGDATABASE='%(db_name)s'
export PGUSER='%(db_user)s'

# start by dropping all the user tables
read -d ' ' ALL_TABLES=<<EOF
buffered_land
land_polygons
mz_pending_path_major_route
ne_10m_admin_0_boundary_lines_land
ne_10m_admin_0_boundary_lines_map_units
ne_10m_admin_1_states_provinces_lines
ne_10m_coastline
ne_10m_lakes
ne_10m_land
ne_10m_ocean
ne_10m_playas
ne_10m_populated_places
ne_10m_roads
ne_10m_urban_areas
ne_110m_admin_0_boundary_lines_land
ne_110m_coastline
ne_110m_lakes
ne_110m_land
ne_110m_ocean
ne_50m_admin_0_boundary_lines_land
ne_50m_admin_1_states_provinces_lines
ne_50m_coastline
ne_50m_lakes
ne_50m_land
ne_50m_ocean
ne_50m_playas
ne_50m_urban_areas
planet_osm_line
planet_osm_nodes
planet_osm_point
planet_osm_polygon
planet_osm_rels
planet_osm_roads
planet_osm_ways
water_polygons
wof_neighbourhood
wof_neighbourhood_placetype
EOF

# exit if psql fails
set -e

for table in $ALL_TABLES; do
    psql -t -c "drop table if exists $table cascade"
done

echo PASS
