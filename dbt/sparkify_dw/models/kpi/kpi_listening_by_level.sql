-- KPI 2 · Music Consumption — share of listening hours Free vs Paid  → 100% stacked bar
-- One row per (week, day_part, level). Share is within the week.
with hours as (

    select
        date_trunc('week', d.full_date)::date          as week_start,
        d.day_part,
        d.day_part_order,
        s.level_name                                   as subscription_level,
        sum(f.listened_sec) / 3600.0                   as listening_hours,
        sum(f.is_song_play)                            as song_plays
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_datetime') }} d     using (datetime_key)
    join {{ ref('dim_subscription') }} s using (subscription_key)
    where f.is_song_play = 1
    group by 1, 2, 3, 4

)

select
    week_start,
    day_part,
    day_part_order,
    subscription_level,
    round(listening_hours::numeric, 2)                                         as listening_hours,
    song_plays,
    round((100 * listening_hours
           / sum(listening_hours) over (partition by week_start))::numeric, 2) as pct_of_week_hours
from hours
order by week_start, day_part_order, subscription_level
