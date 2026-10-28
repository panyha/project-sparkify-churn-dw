-- Location string looks like "Boston-Cambridge-Newton, MA-NH" (metro area, state list).
with locs as (

    select distinct location
    from {{ ref('stg_event_logs') }}
    where location is not null

)

select
    md5(location)                                           as location_key,
    location                                                as location_raw,
    split_part(location, ', ', 1)                           as metro_area,
    split_part(split_part(location, ', ', 1), '-', 1)       as city,
    split_part(location, ', ', 2)                           as states,
    split_part(split_part(location, ', ', 2), '-', 1)       as primary_state,
    'United States'                                         as country
from locs

union all

select '-1', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown', 'Unknown'
