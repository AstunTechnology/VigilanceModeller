-- Function: merge_incident_data(character varying, character varying, character varying, character varying, character varying)

-- DROP FUNCTION merge_incident_data(character varying, character varying, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION merge_incident_data(table_list character varying, output_table character varying, date_column character varying, easting_column character varying, northing_column character varying)
  RETURNS integer AS
$BODY$
DECLARE
	sel TEXT;
BEGIN
	sel:= 'SELECT 
		"' || replace(date_column, '"', '') || '"::date AS date, 
		"' || replace(easting_column, '"', '') || '"::integer AS easting, 
		"'  || replace(northing_column, '"', '') || '"::integer AS northing 
		FROM ';
	EXECUTE 'DROP TABLE IF EXISTS "' || output_table || '" CASCADE;';
	EXECUTE 'CREATE TABLE "' || output_table || '" AS '|| sel || replace(replace(table_list, '"', ''), ',', '
	 UNION ' || sel) ||';';
	
	RETURN 0;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION merge_incident_data(character varying, character varying, character varying, character varying, character varying)
  OWNER TO postgres;
