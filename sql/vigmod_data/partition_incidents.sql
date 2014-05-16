-- Function: partition_incidents(character varying, character varying, character varying)

-- DROP FUNCTION partition_incidents(character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION partition_incidents(source_table character varying, dataset_name character varying, output_table character varying)
  RETURNS integer AS
$BODY$
DECLARE
	cell_size INTEGER;
	minE INTEGER;	
	minN INTEGER;
	maxE INTEGER;
	maxN INTEGER;
	date_first DATE;
	date_last DATE;
	record_count INTEGER;
	sql TEXT;
BEGIN
	sql:= 'SELECT 
	"cell_size","minE","minN", "maxE", "maxN","date_first","date_last"
	FROM meta_data 
	WHERE "dataset_name" = '''||dataset_name||''';';

	EXECUTE sql INTO cell_size, minE, minN, maxE, maxN, date_first, date_last ;

	RAISE NOTICE 'Selecting variables: %', sql;
	
	RAISE NOTICE 'dataset_name: % ', dataset_name;
	RAISE NOTICE 'Variables: cell_size %, minE %, minN %, maxE %, maxN %, date_first %, date_last %', cell_size, minE, minN, maxE, maxN, date_first, date_last ;
	
	IF EXISTS(SELECT * from information_schema.tables where table_name = output_table) THEN
		EXECUTE 'TRUNCATE TABLE "' || output_table ||'";';
	ELSE	
		EXECUTE 'CREATE TABLE "' || output_table || '" (x integer, y integer, t integer);';
		EXECUTE '
			CREATE INDEX "idx_' || output_table || 'X"
			  ON "' || output_table || '"
			  USING btree
			  (x);
			CREATE INDEX "idx_' || output_table || 'Y"
			  ON "' || output_table || '"
			  USING btree
			  (y);
			CREATE INDEX "idx_' || output_table || 'T"
			  ON "' || output_table || '"
			  USING btree
			  (t);
		';
	END IF;
	sql := 'INSERT INTO "' || output_table || '" (
		SELECT 
			((easting - ' || minE || ')/' || cell_size || ')::integer,
			((northing - ' || minN || ')/' || cell_size || ')::integer,
			(date - ''' || date_first || '''::date)/7
		FROM  "' || source_table || '"
		WHERE ' || maxE || ' >= easting 
			AND easting >= ' || minE || '  
			AND ' || maxN || ' >= northing 
			AND northing >= ' || minN || '
			AND ''' || date_last || '''::date >= date 
			AND date >= ''' || date_first || '''::date
	);';
	EXECUTE sql;
	RAISE NOTICE '%',sql;
	
	EXECUTE 'ANALYZE "' || output_table || '";';
	EXECUTE '
		UPDATE meta_data 
		SET ("maxX", "maxY", "maxT", "incidents_table") = 
		((SELECT max(x) FROM "' || output_table || '"), (SELECT max(y) FROM "' || output_table || '"), (SELECT max(t) FROM "' || output_table || '"), ''' || output_table || ''' ) 
		WHERE meta_data.dataset_name = ''' || dataset_name || ''';
	';
	EXECUTE 'SELECT count(x) FROM "'||output_table||'";' INTO record_count;
	RAISE NOTICE 'Incident cells defined - % records created in "%".', record_count::varchar,output_table;
	RETURN record_count;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION partition_incidents(character varying, character varying, character varying)
  OWNER TO postgres;
