# Lume — energy retailer control room (capstone A)

Your job is to design the system. **This scaffold only ships the upstream
vendor mock and the dataset bootstrap.** Everything else — ingest, storage,
the operational dashboard, the analyst SQL surface, monitoring — is yours.

**Start here:** [`BRIEF.md`](./BRIEF.md).

## Bring it up

```bash
make run
```

The first run downloads ~765 MB of London smart-meter readings from the London
Datastore and preprocesses them into parquet inside a docker volume
(`meter-data`). On first run the download dominates (~3 min on a decent
connection); preprocessing scans the full 167M-row source but only keeps the
configured replay window (~30 sec of CPU). Total **~4 min** end to end. Watch
progress:

```bash
docker compose logs -f data-init
```

Subsequent runs reuse the volume and start in seconds. `make reset` wipes the
volume and forces a re-download.

## What's running

| Service        | Purpose                                              | URL                          |
|----------------|------------------------------------------------------|------------------------------|
| `data-init`    | One-shot: download + preprocess LCL dataset.         | (no port; exits when done)   |
| `meter-vendor` | FastAPI mock of the meter vendor. Serves archive + pushes webhooks. | http://localhost:18100 |

Useful endpoints on `meter-vendor`:

- `GET /healthz` — `{ready, simulated_now, subscriptions}`
- `GET /docs` — OpenAPI / Swagger UI
- `GET /stats` — delivery counters, current `simulated_now`
- `GET /readings?since=...&cursor=...&limit=...` — paginated archive
- `POST /subscriptions` `{webhook_url}` — register for live pushes
- `DELETE /subscriptions/{id}` — unregister

### Sample calls

```bash
# Health
curl -s http://localhost:18100/healthz | jq

# Pull the first page of the historical archive
curl -s 'http://localhost:18100/readings?since=2013-12-01T00:00:00Z&limit=5' | jq

# Subscribe a webhook (your service must be reachable from the container —
# use host.docker.internal for a service running on your host).
curl -X POST http://localhost:18100/subscriptions \
     -H 'content-type: application/json' \
     -d '{"webhook_url": "http://host.docker.internal:9999"}'
```

Each push delivers a JSON body shaped like:

```json
{
  "delivery_id": "01HZ...",
  "readings": [
    {"meter_id": "MAC000002", "household_id": "MAC000002",
     "reading_time": "2013-12-01T00:00:00Z", "kwh": 0.123,
     "received_at": "2013-12-01T00:00:11Z"}
  ]
}
```

Your subscriber must respond `200` to ack the batch (the cursor advances only
on 200). Anything else triggers retry with exponential backoff.

## Chaos toggle

The vendor can simulate the real-world ugliness mentioned in `BRIEF.md`
(duplicates, out-of-order readings, late corrections, outages).

```bash
make vendor-chaos   # turn it on
make vendor-calm    # turn it off
```

Knobs live in `.env` (see comments in `.env.example` for what each does).
Restart `meter-vendor` after editing `.env`.

## Replay speed

Default `REPLAY_SECONDS_PER_DAY=3` means one simulated day per 3 wall-clock
seconds — roughly 5,500 readings/sec. If your subscriber can't keep up,
raise the value in `.env` (5 or 10 is reasonable for early development) and
restart `meter-vendor`.

## Adding your own services

The scaffold deliberately ships a thin `compose.yml`. Add services as you
need them — copy snippets from the course's `labs/streaming/compose.yml`,
`labs/monitoring/compose.yml`, etc. There's no opinionated module layout
under `src/` — design as a team and make your choices defensible.

## Editing in VS Code

The course already has a remote-development pattern (see
`scaffolds/streaming-clickstream/README.md`). Add your own
`.devcontainer/devcontainer.json` pointing at whichever container you spin up
for development.

## Data notes (read me before defending)

The LCL dataset has anonymised household IDs, no real postcodes, and the public
bulk CSV doesn't carry the demographic columns. To make the brief's questions
answerable, the preprocessing step (see `vendor/download_and_preprocess.py`)
applies the following transforms:

- **Window filter** — only readings inside `[REPLAY_WINDOW_START - 30 days,
  REPLAY_WINDOW_END]` are kept. The source spans Nov 2011 – Feb 2014; the
  default window keeps Nov 2013 – Feb 2014.
- **Timezone** — source `DateTime` is UK local (BST / GMT). Cast through
  `Europe/London` to UTC so DST transitions land in the right hour.
- **Outlier clamp** — `kwh` outside `[0, 50]` is dropped (negative spikes from
  meter resets, absurdly high values from solar export errors).
- **Household coverage** — households with fewer than 50% of expected readings
  in the replay window are dropped; the trial had households joining and
  leaving across its 27-month run.
- **Postcode area** — synthesised deterministically from `household_id` as
  `N1-A` … `N1-H`. It's a stable grouping key, not a real postcode.
- **ACORN group** — the bulk CSV doesn't carry ACORN. Synthesised
  deterministically from `household_id` with a UK-realistic distribution
  (30% A-E / 30% F-J / 40% K-Q). Behaviour does not actually correlate with
  the synthesised label.
- **Household size** — derived from the ACORN letter via the published
  proxy table.
- **Weather** — hourly Heathrow observations from NOAA Global Hourly (ISD).
