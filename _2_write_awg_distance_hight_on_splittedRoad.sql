update splitted_roads_i set avg_height = null;
update splitted_roads_i set avg_distance = null;

with height_table as ( select 
    r.id AS road_id,
    AVG(rp.height) AS avrg
FROM 
    splitted_roads_i r
JOIN 
    road_points rp 
ON 
    r.id = rp.road_id
GROUP BY 
    r.id)
	update splitted_roads_i set avg_height = height_table.avrg from height_table
	where height_table.road_id = splitted_roads_i.id;


with distance_table as ( select 
    r.id AS road_id,
    AVG(rp.distance_to_road) AS avrg
FROM 
    splitted_roads_i r
JOIN 
    road_points rp 
ON 
    r.id = rp.road_id
GROUP BY 
    r.id)
	update splitted_roads_i set avg_distance = distance_table.avrg from distance_table
	where distance_table.road_id = splitted_roads_i.id;