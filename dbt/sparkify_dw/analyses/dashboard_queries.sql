-- Ready-to-paste SQL for Metabase "Native query" questions (schema names after `dbt build`).
-- dbt does not run files in analyses/; `dbt compile` just renders them.

-- 1) User Engagement · Line chart  (x = full_date, y = dau, mau_rolling_30d)
select full_date, dau, mau_rolling_30d, stickiness_pct
from kpi.kpi_user_activity_daily
order by full_date;

-- 2) Music Consumption · 100% stacked bar  (x = week_start, series = subscription_level, y = listening_hours)
select week_start, subscription_level, sum(listening_hours) as listening_hours
from kpi.kpi_listening_by_level
group by 1, 2
order by 1, 2;

-- 3) Content Popularity · Horizontal bar  (filter day_part with a dashboard filter)
select content_name, plays
from kpi.kpi_top_content_by_daypart
where content_type = 'Song' and day_part = 'Evening'
order by rank_in_day_part;

-- 4) Customer Retention · Funnel  (Metabase "Funnel" visualization: step = stage, measure = users)
select stage, users
from kpi.kpi_churn_funnel
where current_level = 'all'
order by stage_order;

-- 5) Free-tier · KPI cards (one "Number" card per column)
select ad_impressions, free_users_reached_by_ads, ad_to_upgrade_conversion_pct, est_ad_revenue_usd
from kpi.kpi_free_tier_ads;

-- Heatmap of listening (Pivot table: rows = day_name, columns = hour_of_day, value = song_plays)
select day_of_week, day_name, hour_of_day, sum(song_plays) as song_plays
from kpi.kpi_listening_heatmap
group by 1, 2, 3
order by 1, 3;

-- Churn Risk Monitor · table of active users most likely to leave next
select user_id, current_level, churn_risk_score, songs_per_session, skip_rate_pct,
       thumbs_down_share_pct, ads_per_100_songs, support_events, active_days
from kpi.kpi_user_churn_features
where not is_churned and churn_risk_score >= 40
order by churn_risk_score desc;
