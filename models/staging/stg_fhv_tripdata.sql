{{ config(materialized="view") }}


select
    -- identifiers
    {{ dbt_utils.surrogate_key(["dispatching_base_num", "pickup_datetime"]) }}
    as tripid,
    cast(pulocationid as integer) as pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    cast(dispatching_base_num as string) dispatching_base_num,
    cast(affiliated_base_number as string) affiliated_base_numberl,
    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    -- trip info
    sr_flag
-- dbt build --m <model.sql> --var 'is_test_run: false'
from {{ source("staging", "fhv_2019") }}
where extract(YEAR FROM pickup_datetime)=2019