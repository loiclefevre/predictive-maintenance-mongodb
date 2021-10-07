-- Analyse Devices JSON data using basic SQL
-- see raw data
select * from devices d;

-- see JSON as text
select json_serialize( d.json_document ) from devices d;

-- see JSON as text with pretty formating
select json_serialize( d.json_document pretty ) from devices d;

-- check consistency between primary key and MongoDB ObjectId
select id as "Table ID primary key", d.json_document."_id" as "MongoDB ObjectId" from devices d;

-- use SQL dot notation to access single JSON fields
select 
       d.json_document.id_machine,
       d.json_document.state
  from devices d;

-- do some sorting
  select 
         d.json_document.id_machine,
         d.json_document.state
    from devices d
order by d.json_document.state;

-- do some aggregation
  select 
         d.json_document.state,
         count(*)
    from devices d
group by d.json_document.state
order by 2 desc;

-- include groups filtering clause using having
  select 
         d.json_document.state,
         count(*)
    from devices d
group by d.json_document.state
  having count(*) > 50
order by 2 desc;

-- limits to 3 first states
  select 
         d.json_document.state,
         count(*)
    from devices d
group by d.json_document.state
  having count(*) > 50
order by 2 desc
fetch first 3 rows only;

-- transform back into JSON documents
  select json_object(
         'state': d.json_document.state,
         'machinesDeployed': count(*)
        )
    from devices d
group by d.json_document.state
  having count(*) > 50
order by count(*) desc
fetch first 3 rows only;

-- create an array of JSON objects but here an array with one document
  select json_array( json_object(
         'state': d.json_document.state,
         'machinesDeployed': count(*)
        ) )
    from devices d
group by d.json_document.state
  having count(*) > 50
order by count(*) desc
fetch first 3 rows only;

-- create only one array of JSON objects sorted by states with the greatest number of deployed machines
with states_json_data as (
      select json_object(
             'state': d.json_document.state,
             'machinesDeployed': count(*)
            ) as json_document,
            count(*) as machinesDeployed
        from devices d
    group by d.json_document.state
      having count(*) > 50
    order by count(*) desc
    fetch first 3 rows only
   )
select json_arrayagg( json_document ) 
from states_json_data;



-- Transform a collection into a partitioned one
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

-- SQL query to link latest 10 seconds of device metrics with devices spatial locations while predicting the potential of a failure using a machine learning model stored inside the database
select m.id_machine as id, 
        (select json_value( d.json_document, '$.geometry' RETURNING SDO_GEOMETRY ) from devices d where d.json_document.id_machine.string() = m.id_machine) as geometry,
        -- Machine Learning scoring over features from JSON fields
        PREDICTION(PM_RF_AUTOML USING m.*) as prediction,
        -- Machine Learning scoring over features from JSON fields
        PREDICTION_probability(PM_RF_AUTOML USING m.*) as prediction_probability
-- view retrieving 10 latest seconds of device metrics
from device_metrics_view m;


-- SQL query to use for APEX Heatmap plugin, using the GeoJSON and schema-on-read approach (e.g. parsing the GeoJSON data for each query execution)
-- Duration: 2-3 seconds
with data as (
       select m.id_machine as id, 
--              (select d.geometry from devices_relational d where m.id_machine = d.id_machine) as geometry,
              (select json_value( d.json_document, '$.geometry' RETURNING SDO_GEOMETRY ) from devices d where d.json_document.id_machine.string() = m.id_machine) as geometry,
              -- Machine Learning scoring over features from JSON fields
              PREDICTION(PM_RF_AUTOML USING m.*) as prediction,
              -- Machine Learning scoring over features from JSON fields
              PREDICTION_probability(PM_RF_AUTOML USING m.*) as prediction_probability
        from device_metrics_view m)
-- Select devices for Heatmap (with failure prediction confidence greater than 80%)
select id, geometry, '' as infotip, 'red' as style, 'heat' as layer, 'C9' as markersize from data d
where d.prediction = 1 and prediction_probability >= 0.8
union all
-- Select all devices for green/red markers
select id, geometry, 'Device '||id|| ' (confidence:' || to_char(100*prediction_probability,'999.9') || '%)' as infotip, 
       case when prediction = 0 or prediction_probability < 0.8 then 'green' else 'red' end as style, 
       '1' as layer, 
       'C9' as markersize 
from data d;

-- SQL query to use for APEX Heatmap plugin, using the GeoJSON and schema-on-write approach (e.g. GeoJSON are parsed once and persisted into a relational table with a SDO_GEOMETRY column)
-- Duration: 0.1-0.2 seconds, 20x faster
with data as (
       select m.id_machine as id, 
              (select d.geometry from devices_relational d where m.id_machine = d.id_machine) as geometry,
--              (select json_value( d.json_document, '$.geometry' RETURNING SDO_GEOMETRY ) from devices d where d.json_document.id_machine.string() = m.id_machine) as geometry,
              -- Machine Learning scoring over features from JSON fields
              PREDICTION(PM_RF_AUTOML USING m.*) as prediction,
              -- Machine Learning scoring over features from JSON fields
              PREDICTION_probability(PM_RF_AUTOML USING m.*) as prediction_probability
        from device_metrics_view m)
-- Select devices for Heatmap (with failure prediction confidence greater than 80%)
select id, geometry, '' as infotip, 'red' as style, 'heat' as layer, 'C9' as markersize from data d
where d.prediction = 1 and prediction_probability >= 0.8
union all
-- Select all devices for green/red markers
select id, geometry, 'Device '||id|| ' (confidence:' || to_char(100*prediction_probability,'999.9') || '%)' as infotip, 
       case when prediction = 0 or prediction_probability < 0.8 then 'green' else 'red' end as style, 
       '1' as layer, 
       'C9' as markersize 
from data d;
