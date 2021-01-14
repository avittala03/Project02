CREATE EXTERNAL TABLE IF NOT EXISTS ${hivedb}.${hivetable}
STORED AS AVRO
TBLPROPERTIES ('avro.schema.url'='/pr/hdfsapp/ve2/pdp/pndo/phi/no_gbd/r000/bin/schema/AVRO/${tdDB}/${hivetable}/${hivetable}.avsc');
