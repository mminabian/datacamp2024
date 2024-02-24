with 

tripdata as (

    select *,
    row_number() over (partition by dispatching_base_num,pickup_datetime) as rn
    
    from {{ source('staging', 'fhv-taxi') }}
    Where dispatching_base_num is not null

)

select
    {{dbt_utils.generate_surrogate_key(['dispatching_base_num','pickup_datetime'])}} as tripid,
     dispatching_base_num,
     cast(pickup_datetime as timestamp) as pickup_datetime,
     cast(dropoff_datetime as timestamp) as dropoff_datetime,
     {{dbt.safe_cast("pulocationid",api.Column.translate_type("integer"))}} as pickup_locationid,
     {{dbt.safe_cast("dolocationid",api.Column.translate_type("integer"))}} as dropoff_locationid,
     {{dbt.safe_cast("sr_flag", api.Column.translate_type("integer"))}} as sr_flag,
     affiliated_base_number
from tripdata
--where rn = 1
-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}    
    
