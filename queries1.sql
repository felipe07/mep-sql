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