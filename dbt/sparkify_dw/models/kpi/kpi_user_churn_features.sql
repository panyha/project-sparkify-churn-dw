-- Business question 2: which behaviour warns that a user is about to churn?
-- One row per user, measured over the last N days of their activity
--   (churned users: N days before cancelling; others: N days before their last event)
-- plus a simple rule-based risk score for the "Churn Risk Monitor".
with users as (

    select user_key, user_id, current_level, is_churned, tenure_days,
           coalesce(churned_at, last_event_at) as reference_ts
    from {{ ref('dim_user') }}

),

window_events as (

    select f.*
    from {{ ref('fct_user_events') }} f
    join users u using (user_key)
    where f.event_ts >  u.reference_ts - interval '{{ var("churn_lookback_days") }} days'
      and f.event_ts <= u.reference_ts
      and f.is_churn = 0 and f.is_cancel_visit = 0      -- exclude the cancellation itself

),

features as (

    select
        user_key,
        count(distinct session_id)                                         as sessions,
        count(distinct event_ts::date)                                     as active_days,
        sum(is_song_play)                                                  as song_plays,
        round(sum(coalesce(listened_sec, 0)) / 3600.0, 2)                  as listening_hours,
        sum(is_thumbs_up)                                                  as thumbs_up,
        sum(is_thumbs_down)                                                as thumbs_down,
        sum(is_add_to_playlist)                                            as add_to_playlist,
        sum(is_ad_impression)                                              as ad_impressions,
        sum(is_error)                                                      as errors,
        sum(is_help_visit)                                                 as help_visits,
        sum(is_downgrade)                                                  as downgrades,
        sum(is_skipped)                                                    as skips
    from window_events
    group by user_key

),

rates as (

    select
        u.user_id,
        u.current_level,
        u.is_churned,
        u.tenure_days,
        coalesce(f.sessions, 0)                                                     as sessions,
        coalesce(f.active_days, 0)                                                  as active_days,
        coalesce(f.song_plays, 0)                                                   as song_plays,
        coalesce(f.listening_hours, 0)                                              as listening_hours,
        round(coalesce(f.song_plays, 0)::numeric / nullif(f.sessions, 0), 1)       as songs_per_session,
        round(100.0 * f.skips / nullif(f.song_plays, 0), 1)                          as skip_rate_pct,
        round(100.0 * f.thumbs_down / nullif(f.thumbs_up + f.thumbs_down, 0), 1)   as thumbs_down_share_pct,
        round(100.0 * f.add_to_playlist / nullif(f.song_plays, 0), 2)               as playlist_adds_per_100_songs,
        round(100.0 * f.ad_impressions / nullif(f.song_plays, 0), 2)                as ads_per_100_songs,
        coalesce(f.errors, 0) + coalesce(f.help_visits, 0)                         as support_events,
        coalesce(f.downgrades, 0)                                                   as downgrades
    from users u
    left join features f using (user_key)

)

select
    *,
    -- rule-based score 0–100 built from the signals compared in kpi_churn_signals
      (case when coalesce(thumbs_down_share_pct, 0) >= 35 then 25 else 0 end)
    + (case when coalesce(skip_rate_pct, 0)        >= 20 then 20 else 0 end)
    + (case when coalesce(songs_per_session, 0)    <  15 then 20 else 0 end)
    + (case when coalesce(ads_per_100_songs, 0)    >= 10 then 15 else 0 end)
    + (case when support_events                    >= 2  then 10 else 0 end)
    + (case when active_days                       <= 3  then 10 else 0 end)  as churn_risk_score
from rates
