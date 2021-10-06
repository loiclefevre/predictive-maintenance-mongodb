-- Transform a SODA collection into a partitioned one
alter table device_metrics modify
  PARTITION BY RANGE (CREATED_ON) INTERVAL (INTERVAL '1' MINUTE)
  ( PARTITION part_01 values LESS THAN (TO_TIMESTAMP('01-SEP-2021','DD-MON-YYYY')) ) ONLINE
  UPDATE INDEXES;

-- Add a local index
create index idx_dm_created_on on device_metrics (created_on desc, json_document.id_machine.string()) local;

-- View to retrieve last 10 seconds of data
create or replace view DEVICE_METRICS_VIEW as 
with latest_metrics as (
        select d2.json_document.id_machine.string() as id_machine, 
           max(d2.created_on) as created_on 
      from device_metrics d2 
     where d2.created_on >= current_date - INTERVAL '10' SECOND 
  group by d2.json_document.id_machine.string())
select 
  d.json_document.id_machine.string() as id_machine,
  d.json_document.cast_bar_temperature.number() as cast_bar_temperature,
  d.json_document.solube_oil_temperature.number() as solube_oil_temperature,
  d.json_document.casting_water_temprature.number() as CASTING_WATER_TEMPRATURE,
  d.json_document.flue_gaz_temperature.number() as FLUE_GAS_TEMPERATURE,
  d.json_document.iron_fe.number() as IRON_FE,
  d.json_document.mill_rpm.number() as MILL_RPM,
  d.json_document.cathode_voltage.number() as CATHODE_VOLTAGE,
  d.json_document.casting_rate.number() as CASTING_RATE,
  d.json_document.silicon_si.number() as SILICON_SI,
  d.json_document.ambient_temperature.number() as AMBIENT_TEMPERATURE,
  d.json_document.lube_oil_temperature.number() as LUBE_OIL_TEMPERATURE,
  d.json_document.cast_wheel_speed.number() as CAST_WHEEL_SPEED
from device_metrics d
where d.created_on >= current_date - INTERVAL '10' SECOND 
  and d.created_on = (select d2.created_on 
                        from latest_metrics d2 
                       where d2.id_machine=d.json_document.id_machine.string());

-- Relational table to store static machines information
drop table devices_relational purge;

create table devices_relational (
    id_machine varchar2(50) not null primary key,
    country varchar2(100) not null,
    state varchar2(100) not null,
    city varchar2(100) not null,
    geometry sdo_geometry not null
);

insert into devices_relational
select d.json_document.id_machine.string(),
d.json_document.country.string(),
d.json_document.state.string(),
d.json_document.city.string(),
json_value( d.json_document, '$.geometry' RETURNING SDO_GEOMETRY )
from devices d;

select * from devices_relational;
