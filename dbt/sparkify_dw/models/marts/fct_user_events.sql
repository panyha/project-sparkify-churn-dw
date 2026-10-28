-- FACT: 1 row per event a logged-in user performs (song play or interaction).
-- Additive measures are 0/1 flags + seconds so every KPI is a SUM / COUNT.
with e as (

    select * from {{ ref('stg_event_logs') }}

),

plays as (

    select * from {{ ref('stg_song_plays') }}

)

select
    -- keys
    e.event_id,
    to_char(date_trunc('hour', e.event_ts), 'YYYYMMDDHH24')::bigint       as datetime_key,
    md5(e.user_id)                                                         as user_key,
    case when e.page = 'NextSong' and e.song is not null
         then md5(concat_ws('|', e.song, e.artist)) else '-1' end          as song_key,
    case when e.page = 'NextSong' and e.artist is not null
         then md5(e.artist) else '-1' end                                  as artist_key,
    coalesce(md5(e.user_agent), '-1')                                      as device_key,
    coalesce(md5(e.location), '-1')                                        as location_key,
    md5(e.level)                                                           as subscription_key,
    md5(e.page)                                                            as interaction_type_key,

    -- degenerate dimensions
    e.user_id,
    e.session_id,
    e.item_in_session,
    e.event_ts,

    -- measures
    e.song_length_sec,
    p.listened_sec,
    (e.page = 'NextSong')::int                                             as is_song_play,
    coalesce(p.is_skipped, false)::int                                     as is_skipped,
    (e.page = 'Thumbs Up')::int                                            as is_thumbs_up,
    (e.page = 'Thumbs Down')::int                                          as is_thumbs_down,
    (e.page = 'Add to Playlist')::int                                      as is_add_to_playlist,
    (e.page = 'Roll Advert')::int                                          as is_ad_impression,
    (e.page = 'Submit Upgrade')::int                                       as is_upgrade,
    (e.page = 'Submit Downgrade')::int                                     as is_downgrade,
    (e.page = 'Cancel')::int                                               as is_cancel_visit,
    (e.page = 'Cancellation Confirmation')::int                            as is_churn,
    (e.page = 'Error')::int                                                as is_error,
    (e.page = 'Help')::int                                                 as is_help_visit
from e
left join plays p using (event_id)
