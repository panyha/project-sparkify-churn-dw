-- KPI 3 · Content Popularity — Top 10 songs and artists in each time of day → Horizontal bar
with song_plays as (

    select
        d.day_part, d.day_part_order,
        'Song'                                      as content_type,
        s.song_title || ' — ' || s.artist_name      as content_name,
        sum(f.is_song_play)                         as plays,
        count(distinct f.user_id)                   as listeners
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_datetime') }} d using (datetime_key)
    join {{ ref('dim_song') }} s     using (song_key)
    where f.is_song_play = 1
    group by 1, 2, 3, 4

),

artist_plays as (

    select
        d.day_part, d.day_part_order,
        'Artist'                                    as content_type,
        a.artist_name                               as content_name,
        sum(f.is_song_play)                         as plays,
        count(distinct f.user_id)                   as listeners
    from {{ ref('fct_user_events') }} f
    join {{ ref('dim_datetime') }} d using (datetime_key)
    join {{ ref('dim_artist') }} a   using (artist_key)
    where f.is_song_play = 1
    group by 1, 2, 3, 4

),

ranked as (

    select
        *,
        row_number() over (
            partition by content_type, day_part order by plays desc, content_name
        ) as rank_in_day_part
    from (select * from song_plays union all select * from artist_plays) x

)

select * from ranked
where rank_in_day_part <= 10
order by content_type, day_part_order, rank_in_day_part
