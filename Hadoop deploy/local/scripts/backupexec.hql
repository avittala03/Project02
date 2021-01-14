set hive.exec.dynamic.partition.mode=nonstrict;
use ${hivedb};
insert overwrite table ${bkptablename} partition(run_id) select * from ${sourcetablename};
