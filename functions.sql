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