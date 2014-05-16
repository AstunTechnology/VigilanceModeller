-- Function: risk_matrix_to_csv(character varying, character varying)

-- DROP FUNCTION risk_matrix_to_csv(character varying, character varying);

CREATE OR REPLACE FUNCTION risk_matrix_to_csv(dataset_name character varying, filename character varying)
  RETURNS integer AS
$BODY$
DECLARE
	matrix VARCHAR;
BEGIN
	SELECT INTO matrix matrix_table FROM meta_data WHERE meta_data.dataset_name = dataset_name;
	EXECUTE '
		COPY "'||matrix||'" 
		TO '''||filename||'''
		WITH CSV 
			HEADER 
			DELIMITER '',''
			QUOTE ''"''
		;
	';
	RETURN 0;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION risk_matrix_to_csv(character varying, character varying)
  OWNER TO postgres;
