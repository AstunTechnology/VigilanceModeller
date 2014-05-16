-- Function: select_risks(character varying, character varying)

-- DROP FUNCTION select_risks(character varying, character varying);

CREATE OR REPLACE FUNCTION select_risks(dataset_name character varying, grid_date character varying)
  RETURNS SETOF record AS
$BODY$
DECLARE
	matrix NAME;
	rec_row RECORD;
	date_date DATE;
	week INTEGER;
BEGIN
	date_date:= grid_date::date;
	SELECT INTO matrix matrix_table FROM meta_data WHERE meta_data.dataset_name = dataset_name;
	SELECT INTO week (date_date - date_first)/7 FROM meta_data WHERE meta_data.dataset_name = dataset_name;
	FOR rec_row IN EXECUTE 'SELECT easting_centroid,northing_centroid,geom, risk_intensity FROM "'||matrix||'" WHERE t = '||week||';' LOOP
		RETURN NEXT rec_row;
	END LOOP;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100
  ROWS 1000;
ALTER FUNCTION select_risks(character varying, character varying)
  OWNER TO postgres;
