-- What the user did (page/action) grouped into behaviour categories.
select
    md5(page)                 as interaction_type_key,
    page,
    interaction_category,
    interaction_description_th,
    is_song_play,
    is_feedback,
    is_positive_feedback,
    is_negative_feedback,
    is_subscription_event,
    is_churn_signal
from {{ ref('interaction_types') }}
