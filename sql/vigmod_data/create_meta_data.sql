-- Function: create_meta_data(character varying, integer, character varying, character varying, character varying)

-- DROP FUNCTION create_meta_data(character varying, integer, character varying, character varying, character varying);

CREATE OR REPLACE FUNCTION create_meta_data(dataset character varying, size integer, bounding_box character varying, start_date character varying, end_date character varying)
  RETURNS integer AS
$BODY$
DECLARE
	record_count INTEGER;
BEGIN
	IF NOT EXISTS(SELECT * from information_schema.tables where table_name = 'meta_data') THEN
        
		CREATE TABLE meta_data (
			"dataset_name" varchar NOT NULL, 
			"cell_size" integer NOT NULL,
			"minE" integer NOT NULL,
			"minN" integer NOT NULL,
			"maxE" integer NOT NULL,
			"maxN" integer NOT NULL,
			"date_first" date NOT NULL,
			"date_last" date NOT NULL,
			"maxX" integer,
			"maxY" integer,
			"maxT" integer,
			"incidents_table" varchar,
			"impact_table" varchar,
			"matrix_table" varchar,
			CONSTRAINT meta_data_pkey PRIMARY KEY (dataset_name)
		);

		RAISE INFO 'Meta data table created.';
	END IF;

	BEGIN
		INSERT INTO meta_data("dataset_name", "cell_size", "minE", "minN", "maxE", "maxN", "date_first", "date_last") 
		VALUES (
			dataset, 
			size, 
			trim(both ' ' from split_part(bounding_box, ',', 1))::integer, 
			trim(both ' ' from split_part(bounding_box, ',', 2))::integer, 
			trim(both ' ' from split_part(bounding_box, ',', 3))::integer, 
			trim(both ' ' from split_part(bounding_box, ',', 4))::integer, 
			start_date::date, 
			end_date::date
		);
		RAISE INFO 'Meta data record added.';
        EXCEPTION WHEN unique_violation THEN
            UPDATE meta_data 
            SET ("dataset_name", "cell_size", "minE", "minN", "maxE", "maxN", "date_first", "date_last") =
		(
			dataset, 
			size, 
			trim(both ' ' from split_part(bounding_box, ',', 1))::integer, 
			trim(both ' ' from split_part(bounding_box, ',', 2))::integer, 
			trim(both ' ' from split_part(bounding_box, ',', 3))::integer, 
			trim(both ' ' from split_part(bounding_box, ',', 4))::integer, 
			start_date::date, 
			end_date::date
		)
		WHERE dataset_name = dataset;
		RAISE INFO 'Meta data record updated.';
        END;
	EXECUTE 'SELECT count(dataset_name) FROM "meta_data";' INTO record_count;
	RAISE INFO 'Metadata update to ''%'' dataset.', dataset;
	RETURN record_count;
	
	RETURN 0;
END;
$BODY$
  LANGUAGE plpgsql VOLATILE
  COST 100;
ALTER FUNCTION create_meta_data(character varying, integer, character varying, character varying, character varying)
  OWNER TO postgres;
