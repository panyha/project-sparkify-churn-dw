-- Business question 1: how do users listen in each time of day?  → Heatmap (day × hour)
select
    d.day_of_week,
    d.day_name,
    d.hour_of_day,
    d.day_part,
    s.level_name                                                     as subscription_level,
    sum(f.is_song_play)                                              as song_plays,
    count(distinct f.user_id)                                        as active_users,
    round((sum(f.listened_sec) / 3600.0)::numeric, 2)                as listening_hours,
    round(100.0 * sum(f.is_skipped) / nullif(sum(f.is_song_play), 0), 2) as skip_rate_pct,
    sum(f.is_ad_impression)                                          as ad_impressions
from {{ ref('fct_user_events') }} f
join {{ ref('dim_datetime') }} d     using (datetime_key)
join {{ ref('dim_subscription') }} s using (subscription_key)
group by 1, 2, 3, 4, 5
