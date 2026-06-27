-- =============================================================================
-- Lume Control Room — שש שאילתות ההגנה
-- =============================================================================
-- כל השאילתות מריצות על PostgreSQL 16 (lume DB).
-- הנחה: כל הזמנים ב-TIMESTAMPTZ (UTC). ינואר 2014 = UTC = GMT (אין BST).
-- מריצים מול: psql postgresql://lume:lume@localhost:15432/lume
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- שאלה 1 — צריכה כוללת בכל חלון התיישבות, ערב 13 בינואר 2014 (17:00–22:00)
-- ─────────────────────────────────────────────────────────────────────────────
-- מטרה: 10 חלונות של 30 דקות × סכום קוט"ש כל הלקוחות.
-- בודק: שלמות נתונים, חישוב חלון נכון, GROUP BY זמן.
-- נקודות אחיזה: FLOOR על הדקות → תחילת החלון; AT TIME ZONE 'UTC' מגן מפני
--               שינויי אזור שעה עתידיים.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    date_trunc('hour', reading_time AT TIME ZONE 'UTC')
        + FLOOR(EXTRACT(MINUTE FROM reading_time AT TIME ZONE 'UTC') / 30)::int
          * INTERVAL '30 minutes'                     AS window_start,
    ROUND(SUM(kwh)::numeric, 2)                      AS total_kwh,
    COUNT(DISTINCT meter_id)                         AS meters_reporting,
    ROUND(AVG(kwh)::numeric, 4)                      AS avg_kwh_per_reading
FROM  clean_readings
WHERE reading_time >= '2014-01-13 17:00:00+00'
  AND reading_time <  '2014-01-13 22:00:00+00'
GROUP BY window_start
ORDER BY window_start;

-- תוצאה צפויה: 10 שורות (17:00, 17:30, ..., 21:30)
-- total_kwh טיפוסי: 10,000–14,000 לחלון שיא ערב חורף


-- ─────────────────────────────────────────────────────────────────────────────
-- שאלה 2 — עשרת הבתים הצורכים ביותר ב-13 בינואר 2014
-- ─────────────────────────────────────────────────────────────────────────────
-- מטרה: JOIN בין clean_readings ל-homes, מיון לפי סכום יומי.
-- בודק: חיבור דרך meter_id (לא household_id), קוד מיקוד, קבוצת צריכה.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    h.household_id,
    h.meter_id,
    h.postcode_area,
    h.acorn_group,
    h.tariff_type,
    ROUND(SUM(cr.kwh)::numeric, 3)       AS total_kwh_day,
    COUNT(cr.reading_time)               AS readings_count
FROM  clean_readings cr
JOIN  homes h USING (meter_id)
WHERE cr.reading_time >= '2014-01-13 00:00:00+00'
  AND cr.reading_time <  '2014-01-14 00:00:00+00'
GROUP BY h.household_id, h.meter_id, h.postcode_area, h.acorn_group, h.tariff_type
ORDER BY total_kwh_day DESC
LIMIT 10;

-- שים לב: JOIN דרך meter_id, לא household_id.
-- homes_meter_id_idx (unique) מבטיח שה-JOIN יחזיר שורה אחת לכל מונה.
-- readings_count = 48 אם כל החלונות נמדדו (אחרת → מונה נכשל חלק מהיום)


-- ─────────────────────────────────────────────────────────────────────────────
-- שאלה 3 — ממוצע יומי לפי קבוצת צריכה ותעריף
-- ─────────────────────────────────────────────────────────────────────────────
-- מטרה: ניתוח דמוגרפי — האם לקוחות ToU צורכים פחות? האם Adversity מתסרם?
-- בודק: GROUP BY על שתי עמודות מ-homes, AVG על צבירה יומית (תת-שאילתה).
-- הערה על tariff_type: load_dimensions.py כבר ניקה ל-'ToU'/'Std'.
--   CASE נוסף כ-safety net אם יוכנסו נתונים ישירות בעתיד.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    COALESCE(h.acorn_group, 'Unknown')                     AS acorn_group,
    CASE WHEN h.tariff_type = 'ToU' THEN 'ToU' ELSE 'Std' END AS tariff_type,
    COUNT(DISTINCT h.meter_id)                             AS meter_count,
    ROUND(AVG(daily.day_kwh)::numeric, 3)                  AS avg_daily_kwh,
    ROUND(MIN(daily.day_kwh)::numeric, 3)                  AS min_daily_kwh,
    ROUND(MAX(daily.day_kwh)::numeric, 3)                  AS max_daily_kwh
FROM (
    SELECT
        meter_id,
        DATE(reading_time AT TIME ZONE 'UTC') AS reading_date,
        SUM(kwh)                               AS day_kwh
    FROM  clean_readings
    GROUP BY meter_id, DATE(reading_time AT TIME ZONE 'UTC')
) daily
JOIN homes h USING (meter_id)
GROUP BY acorn_group, tariff_type
ORDER BY acorn_group, tariff_type;

-- קריאה לתוצאה: avg_daily_kwh של ToU < Std = לקוחות גמישי-תעריף מתנהגים יעיל יותר.
-- Adversity + Std = הצריכה הגבוהה ביותר לעיתים (בתים גדולים, ישנים, בידוד גרוע).


-- ─────────────────────────────────────────────────────────────────────────────
-- שאלה 4 — כיסוי מונים יומי לאורך כל חלון הסימולציה
-- ─────────────────────────────────────────────────────────────────────────────
-- מטרה: לאתר ימים שבהם חלק מהמונים לא דיווחו.
-- בודק: COUNT(DISTINCT), אחוז כיסוי, שימוש ב-scalar subquery.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    DATE(reading_time AT TIME ZONE 'UTC')   AS reading_date,
    COUNT(DISTINCT meter_id)                AS meters_reporting,
    (SELECT COUNT(*) FROM homes)            AS total_known_meters,
    ROUND(
        100.0 * COUNT(DISTINCT meter_id)
        / NULLIF((SELECT COUNT(*) FROM homes), 0),
        1
    )                                       AS coverage_pct
FROM  clean_readings
GROUP BY DATE(reading_time AT TIME ZONE 'UTC')
ORDER BY reading_date;

-- NULLIF מגן מפני חלוקה באפס אם טבלת homes ריקה.
-- ימים עם coverage_pct < 85% = ירידה משמעותית — כנראה הפסקת שידור.
-- ניתן להוסיף HAVING coverage_pct < 100 כדי לסנן ימים מושלמים.


-- ─────────────────────────────────────────────────────────────────────────────
-- שאלה 5 — אחוז צריכת לילה (00:00–06:00) ללקוחות תעריף גמיש (ToU)
-- ─────────────────────────────────────────────────────────────────────────────
-- מטרה: לבדוק אם לקוחות ToU מזיזים צריכה לשעות זולות.
-- בודק: CASE בתוך SUM (צבירה מותנית), EXTRACT(HOUR), NULLIF.
-- הגדרת "לילה": שעות UTC 0–5 (00:00–05:59). בינואר = UTC = שעון מקומי.
-- ─────────────────────────────────────────────────────────────────────────────

SELECT
    COUNT(DISTINCT cr.meter_id)                              AS tou_meter_count,
    ROUND(SUM(cr.kwh)::numeric, 2)                           AS total_kwh,
    ROUND(
        SUM(CASE
            WHEN EXTRACT(HOUR FROM cr.reading_time AT TIME ZONE 'UTC') < 6
            THEN cr.kwh ELSE 0
        END)::numeric,
        2
    )                                                        AS off_peak_kwh,
    ROUND(
        (100.0
        * SUM(CASE
              WHEN EXTRACT(HOUR FROM cr.reading_time AT TIME ZONE 'UTC') < 6
              THEN cr.kwh ELSE 0
          END)
        / NULLIF(SUM(cr.kwh), 0))::numeric,   -- kwh is double precision; ROUND(.,2) needs numeric
        2
    )                                                        AS off_peak_pct,
    ROUND(
        (100.0
        * SUM(CASE
              WHEN EXTRACT(HOUR FROM cr.reading_time AT TIME ZONE 'UTC') < 6
              THEN cr.kwh ELSE 0
          END)
        / NULLIF(SUM(cr.kwh), 0)
        -
        100.0 * 6 / 24)::numeric,    -- חלק שיש לצפות בו אם הצריכה שווה (25%)
        2
    )                                                        AS shift_vs_uniform_pct
FROM  clean_readings cr
JOIN  homes h USING (meter_id)
WHERE h.tariff_type = 'ToU';

-- shift_vs_uniform_pct > 0 = לקוחות ToU מזיזים יותר צריכה ללילה מהממוצע האחיד.
-- 6 שעות מתוך 24 = 25% ציפייה אחידה.


-- ─────────────────────────────────────────────────────────────────────────────
-- שאלה 6 — קשר טמפרטורה-צריכה: יומי + מקדם מתאם
-- ─────────────────────────────────────────────────────────────────────────────
-- מטרה: להראות שצריכה עולה כשקר — validation של איכות הנתונים.
-- בודק: JOIN עם weather_hourly דרך date_trunc, חלון OVER(), CORR().
-- חיבור: date_trunc('hour', reading_time) = observed_at
--   → כל קריאת חצי שעה מקבלת את מזג האוויר של השעה המתאימה.
-- ─────────────────────────────────────────────────────────────────────────────

WITH daily AS (
    SELECT
        DATE(cr.reading_time AT TIME ZONE 'UTC')   AS reading_date,
        ROUND(AVG(w.temp_c)::numeric, 1)           AS avg_temp_c,
        ROUND(SUM(cr.kwh)::numeric, 0)             AS total_kwh,
        COUNT(DISTINCT cr.meter_id)                AS meters_reporting
    FROM  clean_readings cr
    JOIN  weather_hourly w
        ON w.observed_at = date_trunc('hour', cr.reading_time)
    GROUP BY DATE(cr.reading_time AT TIME ZONE 'UTC')
)
SELECT
    reading_date,
    avg_temp_c,
    total_kwh,
    meters_reporting,
    ROUND(
        CORR(avg_temp_c, total_kwh) OVER ()::numeric,
        3
    )                                              AS r_temp_kwh
FROM  daily
ORDER BY reading_date;

-- r_temp_kwh נמדד: -0.137 על העומס המלא (21.5M קריאות, 89 ימים, טמפ' 2.4–11.5°C).
-- כלומר מתאם שלילי חלש אך בכיוון הצפוי (קר → יותר צריכה): ככל שקר יותר הצריכה
-- היומית הכוללת עולה, אך הקשר חלש כי הצריכה מושפעת מגורמים נוספים (יום בשבוע,
-- הרגלי משק-בית, אורך יום) ולא רק מהטמפרטורה. ימי ינואר הקרים בולטים מול נובמבר
-- המתון אך הפיזור היומי גדול. שים לב: CORR() הוא פירסון r, לא r²; ל-r² → POWER(r,2).
