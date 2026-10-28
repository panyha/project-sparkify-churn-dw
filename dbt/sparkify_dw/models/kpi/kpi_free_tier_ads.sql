-- KPI 5 · Free-Tier Commercial Operations → KPI cards
--   ad impressions, free users reached, ad→upgrade conversion, estimated ad revenue
with free_events as (

    select f.*
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_subscription') }} s using (subscription_key)
    where s.level = 'free'

),

ad_users as (

    select user_id, min(event_ts) as first_ad_ts, sum(is_ad_impression) as ads_seen
    from free_events
    where is_ad_impression = 1
    group by user_id

),

upgrades as (

    select user_id, min(event_ts) as upgrade_ts
    from {{ ref('fct_user_events') }}
    where is_upgrade = 1
    group by user_id

)

select
    (select count(distinct user_id) from free_events)                            as free_users,
    (select sum(is_ad_impression) from free_events)                              as ad_impressions,
    (select count(*) from ad_users)                                              as free_users_reached_by_ads,
    round((select avg(ads_seen) from ad_users), 1)                               as avg_ads_per_reached_user,
    round(100.0 * (select sum(is_ad_impression) from free_events)
          / nullif((select sum(is_song_play) from free_events), 0), 2)           as ads_per_100_free_songs,
    (select count(*) from ad_users a join upgrades u using (user_id)
      where u.upgrade_ts > a.first_ad_ts)                                        as upgraded_after_ads,
    round(100.0 * (select count(*) from ad_users a join upgrades u using (user_id)
                    where u.upgrade_ts > a.first_ad_ts)
          / nullif((select count(*) from ad_users), 0), 2)                       as ad_to_upgrade_conversion_pct,
    round(((select sum(is_ad_impression) from free_events)
           * {{ var('ad_cpm_usd') }} / 1000.0)::numeric, 2)                      as est_ad_revenue_usd,
    {{ var('ad_cpm_usd') }}                                                      as assumed_cpm_usd
