-- Clean + type the raw log. Grain: 1 row per logged-in user event.
with source as (

    select * from {{ source('raw', 'event_logs') }}

),

typed as (

    select
        nullif(trim(user_id), '')                               as user_id,
        session_id,
        item_in_session,
        (to_timestamp(ts / 1000.0) at time zone 'UTC')          as event_ts,
        page,
        auth,
        method,
        status,
        lower(level)                                            as level,
        initcap(first_name)                                     as first_name,
        initcap(last_name)                                      as last_name,
        upper(gender)                                           as gender,
        location,
        trim(both '"' from user_agent)                          as user_agent,
        (to_timestamp(registration / 1000.0) at time zone 'UTC') as registered_at,
        artist,
        song,
        length                                                  as song_length_sec
    from source

),

deduped as (

    select
        *,
        row_number() over (
            partition by user_id, session_id, item_in_session, event_ts, page
            order by event_ts
        ) as _rn
    from typed
    where user_id is not null          -- drop logged-out / guest traffic

)

select
    md5(concat_ws('|', user_id, session_id, item_in_session, event_ts, page)) as event_id,
    user_id,
    session_id,
    item_in_session,
    event_ts,
    page,
    auth,
    method,
    status,
    level,
    first_name,
    last_name,
    gender,
    location,
    user_agent,
    registered_at,
    artist,
    song,
    song_length_sec
from deduped
where _rn = 1
