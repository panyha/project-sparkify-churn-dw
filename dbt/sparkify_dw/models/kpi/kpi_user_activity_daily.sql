-- KPI 1 · User Engagement — DAU / MAU (rolling 30 days) / stickiness  → Line chart
with daily_users as (

    select distinct d.full_date, f.user_id
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_datetime') }} d using (datetime_key)

),

days as (

    select distinct full_date from {{ ref('dim_datetime') }}

),

dau as (

    select
        d.full_date,
        count(distinct f.user_id)                               as dau,
        count(distinct (f.user_id, f.session_id))               as sessions,
        sum(f.is_song_play)                                     as song_plays,
        round(sum(coalesce(f.listened_sec, 0)) / 3600.0, 1)     as listening_hours
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_datetime') }} d using (datetime_key)
    group by d.full_date

),

mau as (

    select
        days.full_date,
        count(distinct u.user_id) as mau_rolling_30d
    from days
    join daily_users u
      on u.full_date between days.full_date - 29 and days.full_date
    group by days.full_date

)

select
    days.full_date,
    coalesce(dau.dau, 0)                                                  as dau,
    mau.mau_rolling_30d,
    round(100.0 * coalesce(dau.dau, 0) / nullif(mau.mau_rolling_30d, 0), 1) as stickiness_pct,
    coalesce(dau.sessions, 0)                                             as sessions,
    coalesce(dau.song_plays, 0)                                           as song_plays,
    coalesce(dau.listening_hours, 0)                                      as listening_hours
from days
left join dau using (full_date)
left join mau using (full_date)
order by days.full_date
