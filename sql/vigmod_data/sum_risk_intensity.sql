-- Function: sum_risk_intensity(character varying, character varying)

-- DROP FUNCTION sum_risk_intensity(character varying, character varying);

CREATE OR REPLACE FUNCTION sum_risk_intensity(dataset_name character varying, output_table character varying)
  RETURNS integer AS
$BODY$
DECLARE
	source_table VARCHAR;
	record_count INTEGER;
	minE INTEGER;
	minN INTEGER;
	cell_size INTEGER;
	date_first DATE;
	sql TEXT;
BEGIN
	sql:= 'SELECT 	
	meta_data."impact_table", meta_data."minE", meta_data."minN", meta_data."cell_size", meta_data."date_first"
	FROM meta_data 
	WHERE "dataset_name" = '''||dataset_name||''';';
	
	EXECUTE sql INTO source_table, minE, minN, cell_size, date_first;
	
	IF EXISTS(SELECT * from information_schema.tables where table_name = output_table) THEN
		EXECUTE 'TRUNCATE TABLE "' || output_table ||'";';
	ELSE	
		EXECUTE 'CREATE TABLE "' || output_table || '" (x integer, y integer, t integer, risk_intensity numeric, easting_centroid integer, northing_centroid integer, week_commencing date);';
		EXECUTE '
			CREATE INDEX "idx_' || output_table || '_t"
			  ON "' || output_table || '"
			  USING btree
			  (t);
			CREATE INDEX "idx_' || output_table || '_wc"
			  ON "' || output_table || '"
			  USING btree
			  (week_commencing);
		';
		EXECUTE 'SELECT AddGeometryColumn(''' || output_table || ''', ''geom'', 27700, ''POLYGON'', 2);';
	END IF;
	EXECUTE '
		INSERT INTO "'||output_table||'"(
				x, y, t, risk_intensity, easting_centroid, northing_centroid, week_commencing, geom
			) 
			SELECT 
				x, 
				y, 
				t, 
				sum(risk_delta), 
				('||minE||'+(x*'||cell_size||')+('||cell_size||'/2)), 
				('||minN||'+(y*'||cell_size||')+('||cell_size||'/2)),
				('''||date_first||'''::date+(t*7))::date,
				ST_GeometryFromText(
					''POLYGON((''||
						'||minE||'+(x*'||cell_size||')||'' ''||'||minN||'+(y*'||cell_size||')||'',''||
						'||minE||'+(x*'||cell_size||')||'' ''||'||minN||'+((y+1)*'||cell_size||')||'',''||
						'||minE||'+((x+1)*'||cell_size||')||'' ''||'||minN||'+((y+1)*'||cell_size||')||'',''||
						'||minE||'+((x+1)*'||cell_size||')||'' ''||'||minN||'+(y*'||cell_size||')||'',''||
						'||minE||'+(x*'||cell_size||')||'' ''||'||minN||'+(y*'||cell_size||')
					||''))'',
					27700
				)
			FROM "'||source_table||'" 
			GROUP BY x,y,t;';
	--Alternatives: SELECT DISTINCT then LOOP, sum or LOOP all records, INSERT/UPDATE 
	EXECUTE 'ANALYZE "' || output_table || '";';
	EXECUTE '
		UPDATE meta_data 
		SET ("matrix_table") = 
		(''' || output_table || ''' ) 
		WHERE meta_data.dataset_name = ''' || dataset_name || ''';
	';
	EXECUTE 'SELECT count(x) FROM "'||output_table||'";' INTO record_count;
	RAISE NOTICE 'Matrix risks aggregated - % records created in "%".', record_count::varchar,output_table;
	RETURN record_count;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION sum_risk_intensity(character varying, character varying)
  OWNER TO postgres;
