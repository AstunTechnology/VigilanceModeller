-- Function: create_risk_matrix(character varying, character varying, integer, character varying, character varying, character varying, integer, integer)

-- DROP FUNCTION create_risk_matrix(character varying, character varying, integer, character varying, character varying, character varying, integer, integer);

CREATE OR REPLACE FUNCTION create_risk_matrix(dataset_name character varying, source_table character varying, cell_size integer, bounding_box character varying, date_first character varying, date_last character varying, spatial_bandwidth integer, temporal_bandwidth integer)
  RETURNS character varying AS
$BODY$
DECLARE
	matrix_table VARCHAR;
	record_count INTEGER;
	bbox varchar;
	mindate varchar;
	maxdate varchar;
BEGIN
	if bounding_box = '' then
		execute 'select min(easting) ||'',''||min(northing) ||'',''||max(easting) ||'',''||max(northing) from "' || source_table || '"' into bbox;
	else
		bbox := bounding_box;
	end if;

	if date_first = '' then
		execute 'select min(date) from "' || source_table || '"' into mindate;
	else
		mindate := date_first;
	end if; 
	if date_last = '' then
		execute 'select max(date) from "' || source_table || '"' into maxdate;
	else
		maxdate := date_last;
	end if; 
	
	matrix_table:= 'dataset_'||dataset_name||'_matrix';

	RAISE INFO '-->Starting create_meta_data(''%'', %, ''%'', ''%'', ''%'')', dataset_name, cell_size, bbox, mindate, maxdate;
	PERFORM create_meta_data(dataset_name, cell_size, bbox, mindate, maxdate);

	RAISE INFO '-->Starting partition_incidents(%, %, %)',source_table, dataset_name, 'dataset_'||dataset_name||'_incidents';
	PERFORM partition_incidents(source_table, dataset_name, 'dataset_'||dataset_name||'_incidents');

	RAISE INFO '-->Starting calculate_incident_impact(%, %, %, %)',dataset_name, 'dataset_'||dataset_name||'_impacts', spatial_bandwidth, temporal_bandwidth;
	PERFORM calculate_incident_impact(dataset_name, 'dataset_'||dataset_name||'_impacts', spatial_bandwidth, temporal_bandwidth);

	RAISE INFO '-->Starting sum_risk_intensity(%, %)', dataset_name, matrix_table;
	PERFORM sum_risk_intensity(dataset_name, matrix_table);
	
	EXECUTE 'SELECT count(x) FROM "'||matrix_table||'";' INTO record_count;
	RAISE INFO 'Matrix created - "%".', matrix_table;
	RETURN matrix_table;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION create_risk_matrix(character varying, character varying, integer, character varying, character varying, character varying, integer, integer)
  OWNER TO postgres;
