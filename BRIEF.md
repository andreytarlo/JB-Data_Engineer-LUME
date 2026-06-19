# Lume Control Room — Project Brief

## Business context

Lume is a small UK challenger energy retailer — twelve thousand domestic
customers, mostly across north and east London, on a mix of standard and
time-of-use tariffs. The company buys electricity wholesale and resells it
to households; the margin on each kWh is thin, and the single largest
controllable cost is the penalty Lume pays the grid operator when its
half-hourly consumption forecast is wrong. Today that penalty is settled
the morning after the fact, from a CSV the grid emails over. By the time
anyone in the office sees it, it is already paid.

The CTO has decided that has to change. The reason it can change now is that
the smart-meter rollout has finally reached enough of Lume's book that a
third-party meter vendor can stream live half-hourly readings to the company
in near-real-time — every household, every thirty minutes, pushed over a
webhook the moment the reading is collected. The same vendor exposes the
historical archive via a paginated REST endpoint so that anything missed
during an outage can be backfilled, and so that analysts can run "what
happened last Tuesday" queries against the same source of truth the live
stream comes from.

Your team's mandate is to build the control room around this feed. The
overnight grid-balancing shift — two people on rotation, plus the duty
analyst on call — needs to see consumption as it lands and be warned before
the half-hour aggregate crosses the threshold that triggers a grid penalty.
The analyst team needs a SQL surface they trust enough to run live queries
against during the morning standup, and to defend at the regulator's
quarterly review. You have three weeks before you defend the system live in
class.

## Data source

The meter vendor exposes two surfaces, both already running as a containerised application
in this scaffold under `meter-vendor`.

**Historical archive.** Paginated REST, cursored. Use this for backfill the history.

```http
GET /readings?since=2013-12-01T00:00:00Z&cursor=<opaque>&limit=1000
→ 200 { "readings": [ {delivery_id, meter_id, household_id, reading_time, kwh, received_at}, ... ],
        "next_cursor": "..." | null }
```

**Live subscription.** Webhook push, use for live data.

```http
POST /subscriptions
     { "webhook_url": "http://your-ingest:port/path" }
→ 200 { "subscription_id": "..." }
```

```http
POST <your webhook_url>
     { "delivery_id": "...", "readings": [ {meter_id, household_id, reading_time, kwh, received_at}, ... ] }
→ 200   (anything else = retry with exponential backoff; the cursor only
         advances on 200, so the contract is at-least-once and you must
         dedupe on delivery_id)
```

The vendor advances a simulated clock at a configurable replay speed so the
historical dataset behaves like a live feed. By default one simulated day
takes three wall-clock seconds; you can slow it down for demos or speed it
up for stress tests via `REPLAY_SECONDS_PER_DAY` in `.env`.

Supporting data is bundled into the same volume the vendor reads from:

- **Customer reference file** — one row per household, with the household's
  ACORN segment, tariff type (Std or ToU), nominal household size, and a
  stable postcode-area label. Use it to enrich readings for the analyst
  surface and for the operational dashboard's by-area breakdowns.
- **Hourly weather feed** — outdoor temperature and a coarse weather code at
  London Heathrow, hourly. Use it to explain consumption variance, especially
  for the questions about cold snaps.

**Things to expect from the provider.** This simulates each of these on
demand (see `make vendor-chaos`); your system must cope without operator
intervention.

- **Duplicate batches.** The same `delivery_id` will sometimes be pushed
  twice. Your dedup is on you.
- **Out-of-order readings within a batch.** Reading timestamps inside a
  single batch are not guaranteed monotonic — the vendor collects from
  multiple regional concentrators in parallel.
- **Outages.** The vendor will stop sending for minutes at a time. Your
  operational alert that says "the feed is gone" must fire fast, and your
  backfill must catch up gracefully once it returns.
- **Late corrections.** A reading from a prior batch will occasionally be
  re-emitted with a corrected `kwh` value (and the same `meter_id` +
  `reading_time`). Your analyst surface must reflect the corrected value, not
  the original.

## Definition of done

Lume's defining the success of this project around two concrete deliverables,
plus an operational baseline they expect of any production-grade system you
hand them.

### Operational dashboard — what the team watches during the shift

The overnight shift sits in front of one screen. The dashboard must be live
enough that the operator does not have to refresh anything to see the next
half-hour roll over. Treat this as an always-on observability surface, not a
one-time report — it must work continuously through the night, survive the
chaos modes above, and keep itself fresh without operator intervention.

- **Feed health.** Is the vendor feed currently healthy? When was the last
  delivery accepted? How many deliveries in the last 5 minutes vs. the
  trailing hour's baseline?
- **Current consumption vs threshold.** The forming half-hour's total kWh
  across the book, against the threshold above which Lume pays a grid
  penalty. Make the breach point unambiguous to a tired operator at 03:00.
- **Peak-forming warning.** A leading indicator — projected end-of-window
  total from the readings seen so far in the current half-hour — so the
  operator gets a heads-up before the breach, not after.
- **Updates within seconds of new readings.** The dashboard must reflect new
  data within a few seconds of the vendor pushing it. Polling-every-minute
  is not acceptable.

### Analyst access — what has happened, and what's happening in detail

Lume's analyst team needs to interrogate the data directly with SQL. The
surface you expose must be fast enough that someone can iterate live during
the morning standup without anyone losing patience, and structured cleanly
enough that the analyst can answer questions you didn't anticipate.

The team should be able to answer questions like:

1. What was the total consumption (kWh) of the entire customer book in each
   half-hour of yesterday evening (17:00-22:00)?
2. Which 10 households had the highest consumption during the cold snap on
   2014-01-13, and which postcode areas do they fall in?
3. For each ACORN segment, what is the average daily consumption over the
   last 30 days, broken down by tariff type?
4. How many distinct meters reported at least one reading on each day of the
   replay window? (Use this to spot meters that have gone silent.)
5. For households on the Time-of-Use tariff, what proportion of their daily
   consumption happens in the off-peak window (00:00-06:00)?
6. Show me the relationship between Heathrow's daily mean temperature and
   the book-wide daily total consumption — does the cold snap show up?

The data behind these answers must be trustworthy. Duplicates from the feed
must not double-count. Late corrections must overwrite the original reading.
Backfills after an outage must not produce a different answer than if the
feed had never dropped.

At your defence, you will provide the SQL queries that answer each of the
questions above and run them live against your system. Bring the queries
written, prepared to explain the data model behind them and the pipeline feeding it.

### Operational baseline

The system must come up with a single command on a fresh laptop — `make run`
in your project root — and survive a `docker compose restart` of any one
service without losing data, dropping a webhook delivery, or requiring a
human to babysit it back to a healthy state. The overnight shift will be
restarting things at 02:00; assume they're tired.
