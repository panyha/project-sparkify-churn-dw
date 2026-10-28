-- Churned vs retained users side by side (same look-back window) → which signals differ most?
select
    case when is_churned then 'Churned' else 'Retained' end           as user_group,
    count(*)                                                          as users,
    round(avg(active_days), 1)                                        as avg_active_days,
    round(avg(songs_per_session), 1)                                  as avg_songs_per_session,
    round(avg(listening_hours), 2)                                    as avg_listening_hours,
    round(avg(skip_rate_pct), 1)                                      as avg_skip_rate_pct,
    round(avg(thumbs_down_share_pct), 1)                              as avg_thumbs_down_share_pct,
    round(avg(playlist_adds_per_100_songs), 2)                        as avg_playlist_adds_per_100_songs,
    round(avg(ads_per_100_songs), 2)                                  as avg_ads_per_100_songs,
    round(avg(support_events), 2)                                     as avg_support_events,
    round(avg(churn_risk_score), 1)                                   as avg_churn_risk_score
from {{ ref('kpi_user_churn_features') }}
group by 1
order by 1
