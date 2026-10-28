-- Device / OS / browser parsed from the user-agent string.
with agents as (

    select distinct user_agent
    from {{ ref('stg_event_logs') }}
    where user_agent is not null

)

select
    md5(user_agent)                                        as device_key,
    user_agent,
    case
        when user_agent ilike '%iPhone%'   then 'iOS'
        when user_agent ilike '%iPad%'     then 'iOS'
        when user_agent ilike '%Android%'  then 'Android'
        when user_agent ilike '%Windows%'  then 'Windows'
        when user_agent ilike '%Macintosh%' then 'macOS'
        when user_agent ilike '%Linux%'    then 'Linux'
        else 'Other'
    end                                                    as operating_system,
    case
        when user_agent ilike '%Edge%'                              then 'Edge'
        when user_agent ilike '%Trident%' or user_agent ilike '%MSIE%' then 'Internet Explorer'
        when user_agent ilike '%Firefox%'                           then 'Firefox'
        when user_agent ilike '%Chrome%'                            then 'Chrome'
        when user_agent ilike '%Safari%'                            then 'Safari'
        else 'Other'
    end                                                    as browser,
    case
        when user_agent ilike '%iPad%' or user_agent ilike '%Tablet%' then 'Tablet'
        when user_agent ilike '%Mobile%' or user_agent ilike '%iPhone%'
             or user_agent ilike '%Android%'                         then 'Mobile'
        else 'Desktop'
    end                                                    as device_type
from agents

union all

select '-1', 'Unknown', 'Unknown', 'Unknown', 'Unknown'
