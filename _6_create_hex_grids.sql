CREATE TABLE IF NOT EXISTS hex10 (
  id SERIAL PRIMARY KEY, 
  hex TEXT, 
  point_count INT, 
  sum_height_width double precision, 
  geom GEOMETRY(Polygon, 4326)
);
INSERT INTO hex10 (
  hex, point_count, sum_height_width, 
  geom
) WITH hexes AS (
  SELECT 
    st_transform(
      st_setsrid(geom, 3857), 
      4326
    ):: point AS point, 
    height / CASE WHEN distance_to_road < 3 THEN 3 ELSE distance_to_road END as ratio 
  FROM 
    road_points
) 
SELECT 
  h3_lat_lng_to_cell(point, 10) AS hex, 
  count(1), 
  SUM(ratio) AS sum_height_width, 
  h3_cell_to_boundary(
    h3_lat_lng_to_cell(point, 10)
  ):: geometry AS geom 
FROM 
  hexes 
GROUP BY 
  hex;
select 
  count(1) 
from 
  buildings_i;