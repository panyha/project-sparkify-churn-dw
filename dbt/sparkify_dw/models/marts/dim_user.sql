-- One row per user with profile + lifecycle attributes (Type 1: latest values).
with events as (

    select * from {{ ref('stg_event_logs') }}

),

latest_profile as (

    select distinct on (user_id)
        user_id, first_name, last_name, gender, registered_at, level as current_level
    from events
    order by user_id, event_ts desc, item_in_session desc

),

lifecycle as (

    select
        user_id,
        min(event_ts)                                                         as first_event_at,
        max(event_ts)                                                         as last_event_at,
        min(event_ts) filter (where page = 'Cancellation Confirmation')       as churned_at,
        min(event_ts) filter (where page = 'Submit Upgrade')                  as first_upgrade_at,
        bool_or(level = 'paid')                                               as ever_paid
    from events
    group by user_id

)

select
    md5(p.user_id)                                         as user_key,
    p.user_id,
    p.first_name,
    p.last_name,
    p.gender,
    p.registered_at,
    p.current_level,
    l.first_event_at,
    l.last_event_at,
    l.churned_at,
    l.churned_at is not null                               as is_churned,
    l.first_upgrade_at,
    l.ever_paid,
    (date_part('day', coalesce(l.churned_at, l.last_event_at) - p.registered_at))::int
                                                           as tenure_days
from latest_profile p
join lifecycle l using (user_id)
