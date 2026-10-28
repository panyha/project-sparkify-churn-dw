-- Artists heard in the log. Key '-1' = "not a song event" (keeps fact FKs non-null).
-- Note: the event log has no artist location; that would come from the separate song metadata dataset.
with artists as (

    select
        artist                         as artist_name,
        count(distinct song)           as songs_in_catalog
    from {{ ref('stg_event_logs') }}
    where page = 'NextSong' and artist is not null
    group by artist

)

select md5(artist_name) as artist_key, artist_name, songs_in_catalog
from artists

union all

select '-1', 'N/A (not a song event)', 0
