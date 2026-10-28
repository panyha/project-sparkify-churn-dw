-- One row per (song, artist). Key '-1' = "not a song event".
with songs as (

    select
        song                                     as song_title,
        artist                                   as artist_name,
        round(avg(song_length_sec)::numeric, 2)  as song_length_sec
    from {{ ref('stg_event_logs') }}
    where page = 'NextSong' and song is not null
    group by song, artist

)

select
    md5(concat_ws('|', song_title, artist_name))   as song_key,
    song_title,
    artist_name,
    md5(artist_name)                               as artist_key,
    song_length_sec,
    case
        when song_length_sec < 180 then 'Short (<3 min)'
        when song_length_sec < 300 then 'Medium (3-5 min)'
        else 'Long (5+ min)'
    end                                            as length_band
from songs

union all

select '-1', 'N/A (not a song event)', 'N/A', '-1', null, 'N/A'
