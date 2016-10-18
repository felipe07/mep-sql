-- PATH TABLE GROUPED BY DEVICE_ID, USER AND LOCATION
SELECT device_id, "user", location, COUNT("user") 
	FROM paths 
	GROUP BY device_id, "user", location
	ORDER BY "user" ASC

-- CREATION OF LINE (USER TRAJECTORY) FROM POINT TRACES
SELECT traces.device_id, traces."user", traces.location, ST_MakeLine(traces.trace_point) AS path 
	FROM (SELECT device_id, "user", location, trace_point 
		FROM paths 
		WHERE device_id LIKE '%11c280e7dc492151%' 
		AND "user" LIKE '%unknown_user%'		
		ORDER BY "timestamp" ASC) AS traces
	GROUP BY traces.device_id, traces."user", traces.location
	
-- CREATION OF LINE (USER TRAJECTORY) FROM POINT TRACES
insert into paths_m1m3m6 (location, type, path)
(SELECT traces.location, 'm1' AS type, ST_MakeLine(traces.point) AS path 
  FROM (
    SELECT location, point 
    FROM centroids_m1
  ) AS traces
GROUP BY traces.location, type)

	
update obstacles 
SET point = s.gpoint
from (
select latitude, longitude, ST_MakePoint(latitude, longitude) as gpoint from obstacles) as s
where obstacles.latitude = s.latitude and obstacles.longitude = s.longitude

-- GET UTM ZONE FROM LAT/LONG GEOMETRY
CREATE OR REPLACE FUNCTION get_utmzone(input_geom geometry)
  RETURNS integer AS
$BODY$
DECLARE
   zone int;
   pref int;
BEGIN
   IF ST_Y(input_geom) >0 THEN
      pref:=32600;
   ELSE
      pref:=32700;
   END IF;
   zone = floor((ST_X(input_geom)+180)/6)+1;
   RETURN zone+pref;
END;
$BODY$
LANGUAGE plpgsql IMMUTABLE;

-- EXAMPLE
SELECT get_utmzone(ST_GeomFromText('POINT(14.21587 40.55405)',4326))

-- GET UTM COORDINATES FROM LAT/LONG GEOMETRY USING ST_GeomFromText
SELECT ST_X(geom), ST_Y(geom) 
FROM 
(SELECT ST_Transform(ST_GeomFromText('POINT(14.21587 40.55405)',4326),32633) as geom) g;

-- GET UTM COORDINATES FROM LAT/LONG GEOMETRY USING ST_SetSrid AND ST_MakePoint
SELECT ST_X(geom), ST_Y(geom) 
FROM 
(SELECT ST_Transform(ST_SetSrid(ST_MakePoint(14.21587, 40.55405), 4326), 32633) as geom) g

-- UPDATE OBSTACLES TABLE WITH POINTS IN UTM 33T (32633) RS
UPDATE obstacles
SET point = s.gpoint FROM
	(SELECT lon, lat, ST_MakePoint(ST_X(geom), ST_Y(geom)) AS gpoint FROM 
		(SELECT coord.longitude AS lon, coord.latitude as lat, ST_Transform(ST_SetSrid(ST_MakePoint(coord.longitude, coord.latitude), 4326), 32633) as geom FROM 
			(SELECT longitude, latitude FROM obstacles) AS coord) g) AS s
WHERE obstacles.latitude = s.lat AND obstacles.longitude = s.lon

-- GENERATE RANDOM NUMBERS
SELECT TRUNC(random() * 3 + 1)

SELECT MIN(i), MAX(i) FROM (
    SELECT TRUNC(random() * 3 + 1) AS i FROM generate_series(1,1000000)
) q

-- GROUP POINTS BY ROUNDINT TO THE Nth (4th IN THIS CASE) DECIMAL NUMBER
SELECT ROUND(CAST(latitude AS NUMERIC), 4) AS lat, ROUND(CAST(longitude AS NUMERIC), 4) AS lon, COUNT(trace_point) AS countp
FROM paths 
WHERE location LIKE '%capri%'
GROUP BY lat, lon

-- LIST ACTIVE QUERIES
SELECT * FROM pg_stat_activity
-- CANCEL QUERY BY PROCESS ID
SELECT pg_cancel_backend(130956)

-- GENERATE RANDOM ACCESSIBILITY LEVELS
DO $$
  DECLARE
    randn double precision;    
  BEGIN
    FOR i IN 1..210824      
    LOOP
      randn := (SELECT trunc(random() * 3 + 1));
      UPDATE paths_test SET accessibility_level = randn where id = i;
    END LOOP;
  END;
$$

-- M1 COMPUTATION 
SELECT d.id, d.location, sum(d.lat * d.acclevel) / sum(d.acclevel) AS latitude, sum(d.lon * d.acclevel) / sum(d.acclevel) AS longitude, ST_MakePoint(longitude, latitude)
FROM
(
  SELECT pt.latitude AS lat, pt.longitude AS lon, pt.accessibility_level AS acclevel, pg.location AS location, pg.id AS id 
  FROM paths_test pt, point_groups pg
  WHERE ROUND(CAST(pt.latitude AS NUMERIC), 4) = pg.latitude
  AND ROUND(CAST(pt.longitude AS NUMERIC), 4) = pg.longitude
) d
GROUP BY d.id, d.location

-- WEIGHTED ARITHMETIC MEAN
INSERT INTO centroids_m1 (
SELECT 
  d.id AS id,
  d.location AS location,
  d.longitude AS longitude, 
  d.latitude AS latitude, 
  ST_MakePoint(d.longitude, d.latitude)
FROM
(
  SELECT 
    pg.id, 
    pg.location, 
    sum(pt.latitude * pt.accessibility_level) / sum(pt.accessibility_level) AS latitude, 
    sum(pt.longitude * pt.accessibility_level) / sum(pt.accessibility_level) AS longitude  
  FROM paths_test pt, point_groups pg
    WHERE ROUND(CAST(pt.latitude AS NUMERIC), 4) = pg.latitude
    AND ROUND(CAST(pt.longitude AS NUMERIC), 4) = pg.longitude 
  GROUP BY pg.location, pg.id
) d )

-- WEIGHTED GEOMETRIC MEAN
INSERT INTO centroids_m3 (
SELECT 
  d.id AS id,
  d.location AS location,
  d.longitude AS longitude, 
  d.latitude AS latitude, 
  ST_MakePoint(d.longitude, d.latitude)
FROM
(
  SELECT 
    pg.id, 
    pg.location, 
    exp(sum(pt.accessibility_level * ln(pt.latitude)) / sum(pt.accessibility_level)) AS latitude, 
    exp(sum(pt.accessibility_level * ln(pt.longitude)) / sum(pt.accessibility_level)) AS longitude    
  FROM paths_test pt, point_groups pg
    WHERE ROUND(CAST(pt.latitude AS NUMERIC), 4) = pg.latitude
    AND ROUND(CAST(pt.longitude AS NUMERIC), 4) = pg.longitude 
  GROUP BY pg.location, pg.id
) d )

-- WEIGHTED HARMONIC MEAN
INSERT INTO centroids_m2 (
SELECT 
  d.id AS id,
  d.location AS location,
  d.longitude AS longitude, 
  d.latitude AS latitude, 
  ST_MakePoint(d.longitude, d.latitude)
FROM
(
  SELECT 
    pg.id, 
    pg.location, 
    sum(pt.accessibility_level) / sum(pt.accessibility_level / pt.latitude) AS latitude, 
    sum(pt.accessibility_level) / sum(pt.accessibility_level / pt.longitude) AS longitude  
  FROM paths_test pt, point_groups pg
    WHERE ROUND(CAST(pt.latitude AS NUMERIC), 4) = pg.latitude
    AND ROUND(CAST(pt.longitude AS NUMERIC), 4) = pg.longitude 
  GROUP BY pg.location, pg.id
) d )

--
with tdata as 
  (select unnest(ARRAY(select point from centroids_m1))::geometry as geom)
select ST_AsText(unnest(ST_ClusterWithin(geom, 1))) FROM tdata;

-- CONVERT TIMESTAMP FIELD TO DATE (TEXT).
SELECT 
  TO_CHAR(TO_TIMESTAMP("timestamp" / 1000), 'YYYY-MM-DD HH24:MI:SS'), 
  id, 
  "timestamp",
  utc_time_acquisition 
FROM 
  paths_fs 
LIMIT 10

-- AGGREGATION OF SUB-PATHS
CREATE OR REPLACE FUNCTION aggregate_sub_paths(table_name regclass, time_gap integer)
  RETURNS void AS
$BODY$
DECLARE
  point_row paths_fs%ROWTYPE;
  subpath_start_id integer;
  subpath_start_timestamp timestamp with time zone;
  current_row_id integer;
  current_row_timestamp timestamp with time zone;
  aux_row_id integer := (SELECT id FROM paths_fs ORDER BY id ASC LIMIT 1);
  aux_row_timestamp timestamp with time zone := (SELECT TO_TIMESTAMP("timestamp" / 1000) FROM paths_fs ORDER BY id ASC LIMIT 1);
BEGIN
  -- Update timestamp_path_start of first row with its own timestamp which is supposed to be the first in time.
  EXECUTE 'UPDATE ' || table_name || ' SET timestamp_path_start = TO_CHAR(''' || aux_row_timestamp || '''::timestamp with time zone, ''YYYY-MM-DD HH24:MI:SS'') WHERE id = ' || aux_row_id; 
  -- The variable subpath_start_id contains the id of the first node of the sub-path, in this case the first row of the table.
  subpath_start_id := aux_row_id;  
  subpath_start_timestamp := aux_row_timestamp;
  -- Iterate over select starting from second row
  FOR point_row IN SELECT * FROM paths_fs ORDER BY id ASC OFFSET 1
  LOOP       
    -- Current row values initialization
    RAISE NOTICE 'index: %', point_row.id;
    current_row_id := point_row.id;	
    SELECT TO_TIMESTAMP(point_row.timestamp / 1000) INTO current_row_timestamp;    
    
    -- Check if the time gap between current row minus the auxiliary row (the row just before current row) is greater than the
    -- parameter 'time gap'
    IF (SELECT EXTRACT(MINUTE FROM (current_row_timestamp - aux_row_timestamp))) > time_gap THEN
      -- Set timestamp_path_end of auxiliary row and every row before it until the beginning of the subpath equal to the timestamp of the
      -- auxiliary row. This is used to set the temporal end of the sub-path i.e. the last point of the sub-path.
      EXECUTE 'UPDATE ' || table_name || ' SET timestamp_path_end = TO_CHAR(''' || aux_row_timestamp || '''::timestamp with time zone, ''YYYY-MM-DD HH24:MI:SS'') WHERE id BETWEEN ' || subpath_start_id || ' AND ' || aux_row_id;
      -- Set the temporal beginning of the sub-path equal to the timestamp of current row. This means another sub-path started.
      EXECUTE 'UPDATE ' || table_name || ' SET timestamp_path_start = TO_CHAR(''' || current_row_timestamp || '''::timestamp with time zone, ''YYYY-MM-DD HH24:MI:SS'') WHERE id = ' || current_row_id;
	  -- The variable subpath_start_id is updated to reflect the fact that a new subpath is recognized.
      subpath_start_id := current_row_id;
	  subpath_start_timestamp := current_row_timestamp;
    ELSE      
      -- If the time gap between current row minus the auxiliary row (the row just before current row) is not greater than the
      -- parameter 'time gap' the timestamp_path_start of the current row is set to the value subpath_start_id. This means the
      -- current point is part of the sub-path composed by the previous points.
      EXECUTE 'UPDATE ' || table_name || ' SET timestamp_path_start = TO_CHAR(''' || subpath_start_timestamp || '''::timestamp with time zone, ''YYYY-MM-DD HH24:MI:SS'') WHERE id = ' || current_row_id;      
    END IF;
    aux_row_id := current_row_id;
    aux_row_timestamp := current_row_timestamp;
  END LOOP;
END;
$BODY$
LANGUAGE plpgsql VOLATILE;