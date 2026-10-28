# Raw event data

The pipeline loads event-log files from the top level of this folder into
`raw.event_logs`. Supported formats are `.json`, `.json.gz`, `.jsonl`, and
`.csv`. JSON files must contain one event per line and use the Sparkify event
schema expected by `scripts/load_raw_events.py`.

## Current data

This folder contains the extracted Udacity Sparkify event logs for November
2018 (`2018-11-01-events.json` through `2018-11-30-events.json`). Keep the
event files directly in this folder; the loader does not search subfolders or
read ZIP archives.

The original archives are kept separately in `reference_data/`:

- `log_data.zip` contains the event logs. Its JSON files can be extracted
  directly into this folder.
- `song_data.zip` contains song metadata. The current pipeline does not load
  or use it; song and artist attributes are currently derived from event logs.

## Using a different event dataset

Put the supported event file(s) directly in this folder. The loader processes
all matching files and combines their rows, so remove or move out any other
dataset first if you do not want the data mixed together. Do not put
`song_data.zip` or its extracted metadata JSON files here; they are not event
logs and do not match the event schema.

Raw event files are ignored by Git. Do not force-add real user data to the
public repository.
