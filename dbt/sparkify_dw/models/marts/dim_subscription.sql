-- Subscription tier (Free / Paid) — the level the user had AT THE TIME of each event.
select
    md5(level)          as subscription_key,
    level,
    level_name,
    is_ad_supported,
    monthly_fee_usd,
    description_th
from {{ ref('subscription_levels') }}
