
update splitted_roads_i set avg_distance = 3 where avg_distance < 3;

--mahalle hesaplamaları

ALTER TABLE adminb ADD COLUMN pop_area double precision;

ALTER TABLE adminb ADD COLUMN building_height_width double precision;

ALTER TABLE adminb ADD COLUMN building_area_area double precision;

update adminb set building_area_area = null;
WITH oran_hesaplama AS (
    SELECT 
        m.fid AS mahalle_id,
        ST_Area(st_transform(m.geom, 3857)) / NULLIF(SUM(ST_Area(b.geom)), 0) AS oran
    FROM 
        adminb m
    LEFT JOIN 
        buildings_i b 
    ON 
        ST_Intersects(st_transform(m.geom, 3857), b.geom)
	where m.admin_level=10
    GROUP BY 
        m.fid, m.geom
)
UPDATE 
    adminb m
SET 
    building_area_area = o.oran
FROM 
    oran_hesaplama o
WHERE 
    m.fid = o.mahalle_id;


update adminb set pop_area = population / st_area(geom);

update adminb set building_height_width = null;

WITH road_avg AS (
    SELECT 
        p.fid AS polygon_id,
        AVG(r.avg_height / NULLIF(r.avg_distance, 0)) AS avg_road_value
    FROM 
        adminb p
    JOIN 
        splitted_roads_i r
    ON 
        ST_Intersects(ST_Transform(p.geom, 3857), r.geom)
    WHERE 
        r.avg_distance IS NOT NULL 
        AND r.avg_distance != 0
        AND r.avg_height IS NOT NULL
		and p.admin_level=10
    GROUP BY 
        p.fid
)
UPDATE 
    adminb p
SET 
    building_height_width = ra.avg_road_value
FROM 
    road_avg ra
WHERE 
    p.fid = ra.polygon_id;

	--points per area

CREATE INDEX IF NOT EXISTS road_points_geom_idx
    ON public.road_points USING gist
    (geom)
    TABLESPACE pg_default;

UPDATE adminb p
SET points_area = subquery.density
FROM (
  SELECT 
    p.fid AS polygon_id,
    COUNT(pt.*)::float / NULLIF(ST_Area(p.geom), 0) AS density
  FROM 
    adminb p
  LEFT JOIN 
    road_points pt
  ON 
    ST_intersects(st_transform(p.geom,3857), pt.geom)
where p.admin_level = 10 
  GROUP BY 
    p.fid, p.geom
) AS subquery
WHERE p.fid = subquery.polygon_id
and p.admin_level = 10;


-----

-- population / area

update adminb set population_area = population / st_area(geom)

update adminb set calc =  5 * COALESCE(population_area, 0) + 6 * COALESCE(points_area, 0) + 
70000 * COALESCE(building_height_width,0) + 40000 * COALESCE(building_area_area,0)

update adminb set calc =  COALESCE(points_area, 0) + 
1000000 * COALESCE(building_height_width,0)



update adminb set calc = null

select COALESCE(population_area, 0) population_area , COALESCE(points_area, 0) points_area,
COALESCE(building_height_width,0) building_height_width, COALESCE(building_area_area,0) building_area_area,*
from adminb where admin_level = 10 limit 5

-- calculate mahalle area
ALTER TABLE public.adminb ADD area float8 NULL;
update adminb set area = st_area(st_transform(geom, 3857)) / 1000 / 1000 where admin_level = 10

-- calculate number of points
ALTER TABLE public.adminb ADD num_of_points int NULL;
UPDATE adminb AS poly
SET num_of_points = sub.point_count
FROM (
    SELECT p.fid,
           COUNT(pt.*) AS point_count
    FROM adminb p
    LEFT JOIN road_points pt
           ON ST_intersects(p.geom, st_transform(pt.geom, 4326))
    where p.admin_level = 10
    GROUP BY p.fid
) AS sub
WHERE poly.fid = sub.fid;

ALTER TABLE public.adminb ADD num_buildings int NULL;

ALTER TABLE public.adminb ADD num_buildings int NULL;

ALTER TABLE public.adminb ADD geom3857 geometry NULL;

update adminb set geom3857 = ST_Transform(geom, 3857) where admin_level = 10
CREATE INDEX idx_build_centroid_geom ON adminb USING GIST(st_centroid(geom));

UPDATE adminb AS poly
SET num_buildings = sub.point_count
FROM (
    SELECT p.fid,
           COUNT(pt.*) AS point_count
    FROM adminb p
    LEFT JOIN buildings_i pt
           ON ST_intersects(p.geom3857, st_centroid(pt.geom))
    where p.admin_level = 10
    GROUP BY p.fid
) AS sub
WHERE poly.fid = sub.fid;
