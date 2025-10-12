select * from adminb where name = 'İstanbul' --adminb = Kontur administrative boundries https://data.humdata.org/dataset/kontur-boundaries

select o.* into roads_i from osm_roads o, adminb a where st_intersects(st_transform(a.geom,3857), o.geometry)
and a.name = 'İstanbul'

select o.* into buildings_i from buildings_with_height o, adminb a where 
st_intersects(st_transform(a.geom,3857), o.geometry)
and a.name = 'İstanbul'

select o.ogc_fid as id, o.height, o.geom into buildings_height_i from  --downloaded from https://zenodo.org/records/11391077
buildings_with_height o, adminb a where st_intersects(a.geom, o.geom)
and a.name = 'İstanbul'

update buildings_with_height set geom = st_setsrid(geom, 4326)

select UpdateGeometrySRID('public', 'buildings_with_height', 'geom', 4326) ;

select buildings_height_i.* into buildings_not_osm from buildings_height_i, buildings_i 
where st_intersects(st_transform(buildings_height_i.geom, 3857), buildings_i.geometry) = false

update buildings_height_i set osm = false;
update buildings_height_i set osm = true from buildings_i 
where st_intersects(buildings_i.geom, st_transform(buildings_height_i.geom, 3857))

CREATE INDEX roads_i_geom_idx ON roads_i USING gist(geometry);

update buildings_i
set height = buildings_height_i.height from buildings_height_i where
st_intersects(buildings_height_i.geom,st_transform(buildings_i.geom, 4326))

alter table adminb alter column geom type geometry (geometry,4326);

insert into buildings_i (height, geom) 
select height, st_transform(geom,3857) from buildings_height_i where osm = false;

---------

update buildings_i
set height = buildings_height_i.height from buildings_height_i where
st_intersects(buildings_height_i.geom,st_transform(buildings_i.geom, 4326))


Alter table adminb alter column geom type geometry (geometry,4326);


insert into buildings_i (height, geom) 
select height, st_transform(geom,3857) from buildings_height_i where osm = false;

----

