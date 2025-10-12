DROP TABLE IF EXISTS road_points CASCADE;

WITH ranked_road_points AS (
    SELECT 
        b.id AS building_id, 
        b.height, 
        ST_ClosestPoint(r.geom, st_centroid(b.geom)) AS nearest_point_on_road, 
        ST_Distance(b.geom, r.geom) AS distance_to_road, --ST_Distance(st_centroid(b.geom), r.geom) AS distance_to_road,
        r.id AS road_id,
        ROW_NUMBER() OVER (
            PARTITION BY b.id 
            ORDER BY ST_Distance(st_centroid(b.geom), r.geom)
        ) AS rank
    FROM 
        buildings_i b, 
        splitted_roads_i r
    WHERE 
        ST_DWithin(b.geom, r.geom, b.height) -- Adjust distance to match proximity
		and 
		r.class = 'highway'
		and r.highway not in (
'cycleway'
'footway'
'pedestrian'
'steps', '') and r.tunnel = 0 and r.bridge = 0 and lower(r.name) not like '%tünel%')


SELECT 
    building_id AS id, 
    road_id, 
    nearest_point_on_road AS geom, 
    height, 
    distance_to_road
INTO 
    road_points
FROM 
    ranked_road_points
WHERE 
    rank <= 2; -- Limit to the nearest two roads


----- line
DROP TABLE IF EXISTS building_lines CASCADE;

CREATE TABLE building_lines AS
SELECT 
    p.id as point_id,
    ST_MakeLine(p.geom, ST_ClosestPoint(st_centroid(b.geom), p.geom)) AS geom,
	b.id as building_id
FROM 
    road_points p
JOIN 
    buildings_i b
ON 
    p.id = b.id;

alter table building_lines add column id serial;
ALTER TABLE IF EXISTS public.building_lines
    ADD CONSTRAINT building_lines_pkey PRIMARY KEY (id);