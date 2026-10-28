"""
Generate a synthetic Sparkify-style event log (JSON Lines) so the whole pipeline
can run without downloading the real Udacity dataset.

The output has exactly the same keys as `mini_sparkify_event_data.json`
(ts, userId, sessionId, page, auth, method, status, level, itemInSession,
location, userAgent, lastName, firstName, registration, gender, artist, song,
length), so the real file can be dropped into raw_data/ without code changes.

Behaviour built into the simulation (so the dashboards have something to find):
  * listening peaks in the evening, dips in the late night
  * free users get "Roll Advert" events; some upgrade after enough ads
  * users who will churn listen less, skip more, give more thumbs down and see
    more ads/errors in the weeks before "Cancellation Confirmation"

Usage:
    python scripts/generate_sample_data.py                       # default 250 users
    python scripts/generate_sample_data.py --users 500 --seed 7 --out raw_data/sparkify_events_sample.json.gz
"""
from __future__ import annotations

import argparse
import gzip
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

START = datetime(2018, 10, 1, tzinfo=timezone.utc)
DAYS = 64  # same window as the real mini dataset (Oct 1 – Dec 3, 2018)

FIRST_NAMES_M = ["Adam", "Brian", "Carlos", "Daniel", "Ethan", "Frank", "George", "Henry", "Isaac", "Jacob",
                 "Kevin", "Liam", "Mason", "Noah", "Oscar", "Peter", "Ryan", "Samuel", "Tyler", "Victor"]
FIRST_NAMES_F = ["Alice", "Bella", "Chloe", "Diana", "Emma", "Fiona", "Grace", "Hannah", "Ivy", "Julia",
                 "Kate", "Lily", "Mia", "Nora", "Olivia", "Paige", "Quinn", "Rose", "Sofia", "Zoe"]
LAST_NAMES = ["Anderson", "Brown", "Clark", "Davis", "Evans", "Garcia", "Harris", "Johnson", "King", "Lee",
              "Martin", "Nguyen", "Owens", "Parker", "Robinson", "Smith", "Taylor", "Walker", "Young", "Wright"]
LOCATIONS = ["Los Angeles-Long Beach-Anaheim, CA", "New York-Newark-Jersey City, NY-NJ-PA", "Boston-Cambridge-Newton, MA-NH",
             "Houston-The Woodlands-Sugar Land, TX", "Chicago-Naperville-Elgin, IL-IN-WI", "Seattle-Tacoma-Bellevue, WA",
             "Phoenix-Mesa-Scottsdale, AZ", "Miami-Fort Lauderdale-West Palm Beach, FL", "Denver-Aurora-Lakewood, CO",
             "Atlanta-Sandy Springs-Roswell, GA", "Bakersfield, CA", "Raleigh, NC", "Tampa-St. Petersburg-Clearwater, FL",
             "Dallas-Fort Worth-Arlington, TX", "Portland-Vancouver-Hillsboro, OR-WA", "Salt Lake City, UT"]
USER_AGENTS = [
    '"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/37.0.2062.103 Safari/537.36"',
    '"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_4) AppleWebKit/537.77.4 (KHTML, like Gecko) Version/7.0.5 Safari/537.77.4"',
    "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:31.0) Gecko/20100101 Firefox/31.0",
    '"Mozilla/5.0 (iPhone; CPU iPhone OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"',
    '"Mozilla/5.0 (iPad; CPU OS 7_1_2 like Mac OS X) AppleWebKit/537.51.2 (KHTML, like Gecko) Version/7.0 Mobile/11D257 Safari/9537.53"',
    "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:31.0) Gecko/20100101 Firefox/31.0",
    '"Mozilla/5.0 (Linux; Android 4.4.2; SM-G900F Build/KOT49H) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.135 Mobile Safari/537.36"',
    "Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0)",
]
ARTIST_A = ["Velvet", "Neon", "Paper", "Silver", "Midnight", "Golden", "Electric", "Crimson", "Hollow", "Quiet",
            "Wild", "Lunar", "Copper", "Glass", "Northern", "Static", "Saffron", "Echo", "Rusty", "Blue"]
ARTIST_B = ["Lanterns", "Harbor", "Foxes", "Avenue", "Satellites", "Orchard", "Parade", "Tides", "Engines", "Choir",
            "Pilots", "Rivers", "Arcade", "Comets", "Gardens", "Signals", "Monsoon", "Brothers", "Owls", "Kites"]
SONG_A = ["Summer", "Broken", "Endless", "Falling", "Golden", "Lonely", "Paper", "Silent", "Burning", "Ocean",
          "Midnight", "Electric", "Dancing", "Distant", "Velvet", "Fading", "Northern", "Sweet", "Hidden", "Wild"]
SONG_B = ["Heart", "Lights", "Road", "Dreams", "Sky", "Rain", "Fire", "Morning", "Story", "Waves",
          "City", "Echoes", "Shadows", "Roses", "Promise", "Thunder", "Letters", "Stars", "Runaway", "Home"]

# hour-of-day weights (UTC): quiet late night, rising through the day, evening peak
HOUR_WEIGHTS = [3, 2, 1.5, 1, 1, 1.5, 3, 5, 6, 6, 6, 6.5, 7, 7, 6.5, 6.5, 7, 8, 9.5, 10, 10, 9, 7, 5]


def build_catalog(rng: random.Random, n_artists: int = 120, songs_per_artist: tuple[int, int] = (3, 12)):
    artists = set()
    while len(artists) < n_artists:
        name = f"{rng.choice(['', 'The '])}{rng.choice(ARTIST_A)} {rng.choice(ARTIST_B)}"
        artists.add(name.strip())
    catalog = []
    for artist in sorted(artists):
        titles = set()
        for _ in range(rng.randint(*songs_per_artist)):
            titles.add(f"{rng.choice(SONG_A)} {rng.choice(SONG_B)}")
        for t in titles:
            catalog.append((artist, t, round(rng.uniform(140, 360), 5)))
    # Zipf-like popularity so "Top 10" charts have a clear head
    weights = [1 / (i + 1) ** 0.9 for i in range(len(catalog))]
    rng.shuffle(catalog)
    return catalog, weights


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def simulate(n_users: int, seed: int):
    rng = random.Random(seed)
    catalog, song_weights = build_catalog(rng)
    events: list[dict] = []
    session_counter = 0

    for uid in range(1, n_users + 1):
        gender = rng.choice("MF")
        first = rng.choice(FIRST_NAMES_M if gender == "M" else FIRST_NAMES_F)
        last = rng.choice(LAST_NAMES)
        location = rng.choice(LOCATIONS)
        agent = rng.choice(USER_AGENTS)
        registration = START - timedelta(days=rng.randint(5, 120), hours=rng.randint(0, 23))
        level = "paid" if rng.random() < 0.35 else "free"
        will_churn = rng.random() < 0.23
        churn_day = rng.randint(15, DAYS - 2) if will_churn else None
        engagement = rng.uniform(0.25, 0.75)       # prob. of a session on a given day
        favourite_hour = rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
        ads_seen_free = 0
        upgrade_threshold = rng.randint(15, 60)    # ads tolerated before considering upgrade

        base = dict(userId=str(uid), firstName=first, lastName=last, gender=gender,
                    location=location, userAgent=agent, registration=ms(registration),
                    auth="Logged In", method="PUT", status=200)

        for day in range(DAYS):
            if churn_day is not None and day > churn_day:
                break
            # churners fade out: fewer sessions, unhappier sessions as churn day approaches
            unhappy = 0.0
            p_session = engagement
            if churn_day is not None:
                closeness = max(0.0, 1 - (churn_day - day) / 21)  # 0 → 1 over the final 3 weeks
                unhappy = closeness
                p_session *= 1 - 0.5 * closeness
            if day != churn_day and rng.random() > p_session:
                continue

            session_counter += 1
            sid = session_counter
            hour = favourite_hour if rng.random() < 0.6 else rng.choices(range(24), weights=HOUR_WEIGHTS)[0]
            t = START + timedelta(days=day, hours=hour, minutes=rng.randint(0, 59), seconds=rng.randint(0, 59))
            item = 0

            def emit(page, **extra):
                nonlocal item
                ev = dict(base, ts=ms(t), sessionId=sid, page=page, level=level, itemInSession=item,
                          artist=None, song=None, length=None)
                if page in ("Home", "About", "Help", "Settings", "Downgrade", "Upgrade", "Cancel"):
                    ev["method"] = "GET"
                ev.update(extra)
                events.append(ev)
                item += 1

            emit("Home")
            t += timedelta(seconds=rng.randint(3, 40))
            n_songs = max(1, int(rng.gauss(22 - 12 * unhappy, 8)))
            for _ in range(n_songs):
                artist, song, length = rng.choices(catalog, weights=song_weights)[0]
                emit("NextSong", artist=artist, song=song, length=length)
                skip_p = 0.10 + 0.30 * unhappy
                listened = length * (rng.uniform(0.05, 0.45) if rng.random() < skip_p else 1.0)
                t += timedelta(seconds=listened)

                r = rng.random()
                if r < 0.045 - 0.025 * unhappy:
                    emit("Thumbs Up")
                elif r < 0.045 + 0.012 + 0.05 * unhappy:
                    emit("Thumbs Down")
                if rng.random() < 0.025 - 0.012 * unhappy:
                    emit("Add to Playlist")
                if rng.random() < 0.01:
                    emit("Add Friend")
                if level == "free" and rng.random() < 0.07 + 0.08 * unhappy:
                    emit("Roll Advert")
                    ads_seen_free += 1
                if rng.random() < 0.002 + 0.01 * unhappy:
                    emit("Error", status=404)
                if rng.random() < 0.005 + 0.015 * unhappy:
                    emit("Help")

            # subscription changes
            if level == "free" and not will_churn and ads_seen_free > upgrade_threshold and rng.random() < 0.15:
                emit("Upgrade")
                emit("Submit Upgrade")
                level = "paid"
            elif level == "paid" and rng.random() < 0.01 + 0.04 * unhappy:
                emit("Downgrade")
                if rng.random() < 0.4:
                    emit("Submit Downgrade")
                    level = "free"
                    ads_seen_free = 0
            if rng.random() < 0.08:
                emit("Settings")
                if rng.random() < 0.3:
                    emit("Save Settings")

            if churn_day == day:
                emit("Cancel")
                t += timedelta(seconds=rng.randint(5, 30))
                emit("Cancellation Confirmation", method="GET")
                break
            emit("Logout")
            # a few logged-out page views (userId == "" like the real data)
            if rng.random() < 0.1:
                events.append(dict(ts=ms(t + timedelta(seconds=5)), userId="", sessionId=sid, page="Home",
                                   auth="Logged Out", method="GET", status=200, level=level, itemInSession=item,
                                   location=None, userAgent=None, firstName=None, lastName=None, gender=None,
                                   registration=None, artist=None, song=None, length=None))
    events.sort(key=lambda e: e["ts"])
    return events


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--users", type=int, default=250)
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=str(Path(__file__).resolve().parent.parent / "raw_data" / "sparkify_events_sample.json.gz"))
    args = ap.parse_args()

    events = simulate(args.users, args.seed)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    opener = gzip.open if out.suffix == ".gz" else open
    with opener(out, "wt", encoding="utf-8") as f:
        for ev in events:
            f.write(json.dumps(ev) + "\n")
    print(f"Wrote {len(events):,} events for {args.users} users → {out}")


if __name__ == "__main__":
    main()
