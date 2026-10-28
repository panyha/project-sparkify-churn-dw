-- Song plays enriched with how long the user actually listened.
-- The log only stores when a song STARTS, so listened time = gap until the next
-- song in the same session (capped at the song length). The last song of a
-- session has no "next" → assume it was played to the end.
with plays as (

    select
        event_id,
        user_id,
        session_id,
        item_in_session,
        event_ts,
        song_length_sec,
        lead(event_ts) over (
            partition by user_id, session_id
            order by event_ts, item_in_session
        ) as next_play_ts
    from {{ ref('stg_event_logs') }}
    where page = 'NextSong'

)

select
    event_id,
    least(
        song_length_sec,
        coalesce(extract(epoch from next_play_ts - event_ts), song_length_sec)
    )::numeric(10, 2)                                                     as listened_sec,
    (next_play_ts is not null
     and extract(epoch from next_play_ts - event_ts)
         < song_length_sec * {{ var('skip_threshold_ratio') }})           as is_skipped
from plays
