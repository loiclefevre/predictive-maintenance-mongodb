-- Transform a SODA collection into a partitioned one
alter table device_metrics modify
  PARTITION BY RANGE (CREATED_ON) INTERVAL (INTERVAL '1' MINUTE)
  ( PARTITION part_01 values LESS THAN (TO_TIMESTAMP('01-SEP-2021','DD-MON-YYYY')) ) ONLINE
  UPDATE INDEXES;

-- Add a local index
create index idx_dm_created_on on device_metrics (created_on desc, json_document.id_machine.string()) local;
