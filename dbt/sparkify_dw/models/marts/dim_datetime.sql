-- date_time dimension at HOUR grain → answers "what do users do at each time of day?"
-- Times are UTC as stored in the source log.
with bounds as (

    select
        date_trunc('day', min(event_ts))                          as start_ts,
        date_trunc('day', max(event_ts)) + interval '23 hours'    as end_ts
    from {{ ref('stg_event_logs') }}

),

hours as (

    select generate_series(start_ts, end_ts, interval '1 hour') as hour_ts
    from bounds

)

select
    to_char(hour_ts, 'YYYYMMDDHH24')::bigint            as datetime_key,
    hour_ts,
    hour_ts::date                                        as full_date,
    to_char(hour_ts, 'YYYYMMDD')::int                    as date_key,
    extract(year from hour_ts)::int                      as year,
    extract(quarter from hour_ts)::int                   as quarter,
    extract(month from hour_ts)::int                     as month,
    to_char(hour_ts, 'Mon')                              as month_name,
    extract(week from hour_ts)::int                      as week_of_year,
    extract(isodow from hour_ts)::int                    as day_of_week,      -- 1 = Mon
    to_char(hour_ts, 'Dy')                               as day_name,
    extract(isodow from hour_ts) in (6, 7)               as is_weekend,
    case when extract(isodow from hour_ts) in (6, 7)
         then 'Weekend' else 'Weekday' end               as day_type,
    extract(hour from hour_ts)::int                      as hour_of_day,
    case
        when extract(hour from hour_ts) between 5  and 11 then 'Morning'
        when extract(hour from hour_ts) between 12 and 16 then 'Afternoon'
        when extract(hour from hour_ts) between 17 and 21 then 'Evening'
        else 'Late Night'
    end                                                  as day_part,
    case
        when extract(hour from hour_ts) between 5  and 11 then 'เช้า'
        when extract(hour from hour_ts) between 12 and 16 then 'บ่าย'
        when extract(hour from hour_ts) between 17 and 21 then 'เย็น'
        else 'ดึก'
    end                                                  as day_part_th,
    case
        when extract(hour from hour_ts) between 5  and 11 then 1
        when extract(hour from hour_ts) between 12 and 16 then 2
        when extract(hour from hour_ts) between 17 and 21 then 3
        else 4
    end                                                  as day_part_order
from hours
