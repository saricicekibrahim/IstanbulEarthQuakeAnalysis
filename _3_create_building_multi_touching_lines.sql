DROP TABLE IF EXISTS building_lines_multiple_touch CASCADE;

WITH line_building_touch AS (
  SELECT 
    l.id AS id,
    b.id AS building_id,
	l.geom as geom,
	l.point_id
  FROM 
    building_lines l
  JOIN 
    buildings_i b
  ON 
    st_intersects(l.geom, b.geom)
)
SELECT 
  id,
  COUNT(DISTINCT building_id) AS building_count,
  geom,
  point_id
INTO
building_lines_multiple_touch
FROM 
  line_building_touch
GROUP BY 
  id,
  geom,
  point_id
HAVING 
  COUNT(DISTINCT building_id) > 1;  