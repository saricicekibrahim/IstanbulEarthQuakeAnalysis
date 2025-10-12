delete from building_lines where id in (select id from building_lines_multiple_touch);
delete from road_points where geom in (


select r.geom from road_points r, building_lines_multiple_touch b 
where 
st_x(r.geom) = st_x(st_startpoint(b.geom)) and
st_y(r.geom) = st_y(st_startpoint(b.geom))
group by r.id, r.road_id, r.geom, r.height, r.distance_to_road
--order by r.id
);

select count(1) from building_lines_multiple_touch