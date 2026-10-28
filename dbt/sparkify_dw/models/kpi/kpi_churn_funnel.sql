-- KPI 4 · Customer Retention — Churn funnel, split by the user's current level → Funnel chart
--   1 Active users
--   2 Dissatisfied      : thumbs-down count > half of thumbs-up count
--   3 Visited Cancel page
--   4 Cancellation confirmed (churned)
-- churn_rate_pct per stage shows how much more likely each behaviour group is to leave
-- than the base rate in stage 1.
with per_user as (

    select
        f.user_id,
        u.current_level,
        sum(f.is_thumbs_down) > sum(f.is_thumbs_up) * 0.5   as is_dissatisfied,
        max(f.is_cancel_visit) = 1                          as visited_cancel,
        max(f.is_churn) = 1                                 as churned
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_user') }} u using (user_key)
    group by 1, 2

),

stages as (

    select current_level, 1 as stage_order, '1. Active users' as stage,
           count(*) as users, count(*) filter (where churned) as churned_users
    from per_user group by 1
    union all
    select current_level, 2, '2. Dissatisfied (many thumbs down)',
           count(*), count(*) filter (where churned)
    from per_user where is_dissatisfied group by 1
    union all
    select current_level, 3, '3. Visited Cancel page',
           count(*), count(*) filter (where churned)
    from per_user where visited_cancel group by 1
    union all
    select current_level, 4, '4. Cancellation confirmed',
           count(*), count(*) filter (where churned)
    from per_user where churned group by 1

),

all_levels as (

    select 'all' as current_level, stage_order, stage, sum(users) as users, sum(churned_users) as churned_users
    from stages group by 2, 3
    union all
    select * from stages

)

select
    current_level,
    stage_order,
    stage,
    users,
    churned_users,
    round(100.0 * churned_users / nullif(users, 0), 1)                          as churn_rate_pct,
    round(100.0 * users / first_value(users) over (
        partition by current_level order by stage_order), 1)                    as pct_of_active
from all_levels
order by current_level, stage_order
