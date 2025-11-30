> End-to-end analytics pipeline in BigQuery using a Bronze -> Silver -> Gold medallion architecture.

All SQL runs in Google BigQuery. Tables are prefixed with `ornate-lead-479415-h3.product_analytics`.

## Table of Contents
- [Overview](#overview)
- [Architecture](#architecture)
- [Data Sources and Known Issues](#data-sources-and-known-issues)
- [Silver Layer Transformations](#silver-layer-transformations)
- [Gold Layer Metrics](#gold-layer-metrics)
- [Experimental Exposure Groups](#experimental-exposure-groups)
- [Key Assumptions](#key-assumptions)
- [Data Quality Audits](#data-quality-audits)
- [Validation Tests](#validation-tests)
- [Limitations](#limitations)
- [Repository Map](#repository-map)

## Overview
This project cleans messy assessment data, standardizes attribution, and produces business-ready lifecycle metrics to evaluate:
- QuickStart impact on activation, retention, and paid conversion
- The `promo_q3` trial campaign
- Performance differences across acquisition channels
- Whether data quality issues bias results

## Architecture
| Layer  | Purpose                                                                                          |
|--------|--------------------------------------------------------------------------------------------------|
| Bronze | Raw ingestion with minimal filtering; preserves full fidelity                                    |
| Silver | Normalization, timestamp parsing, deduplication, attribution logic, canonical flag names         |
| Gold   | Final business metrics, funnel construction, exposure assignment, retention windows              |

## Data Sources and Known Issues
| Dataset              | Main problems found                                                   | Treatment in Silver layer                               |
|----------------------|-----------------------------------------------------------------------|---------------------------------------------------------|
| `users.csv`          | Inconsistent email casing, missing UTM sources, messy countries, inconsistent signup timestamps | Normalize casing, trim/canonicalize UTM, clean countries, unified timestamp parsing |
| `events.csv`         | Mixed timestamp formats (ISO, US short date, epoch ms), missing `user_id`, potential duplicates, events before signup | Parse formats, drop missing IDs, deduplicate, flag/remove pre-signup events          |
| `marketing_clicks.csv` | Clicks without users, UTM mismatches, casing inconsistencies | Clean IDs, lowercase UTMs, parse timestamps, retain click-level UTM for comparison |
| `subscriptions.csv`  | Mixed currencies, optional/NULL end dates, inconsistent dates, subscriptions before signup | Normalize currencies, parse dates, flag anomalies (`end_date < start_date`, `start_date < signup_date`) |
| `feature_flags.csv`  | Multiple QuickStart variants, mixed timestamp formats                | Canonicalize QuickStart names, parse timestamps; pre-signup exposures allowed       |

All identified issues are either corrected in Silver or surfaced through Data Quality audits.

## Silver Layer Transformations
- **`silver_users.sql`**: Normalize emails, canonicalize/trim `utm_source`, clean countries, parse timestamps with `SAFE.PARSE_TIMESTAMP`, deterministic attribution precedence (user value -> earliest pre-signup marketing click -> `Unknown`). Provenance stored as `utm_source_origin` in {`user_table`, `marketing_click_backfill`, `unknown`}.
- **`silver_events.sql`**: Parse ISO/US/epoch timestamps, remove duplicates via `ROW_NUMBER`, drop missing/invalid `user_id`, flag pre-signup events, normalize event names.
- **`silver_marketing_clicks.sql`**: Trim/lowercase UTM params, clean empty `user_id`, parse timestamps, retain click-level UTM.
- **`silver_subscriptions.sql`**: Unified date parsing, currency normalization, flag anomalies (`end_date < start_date`, `start_date < signup_date`) which are excluded downstream.
- **`silver_feature_flags.sql`**: Canonicalize QuickStart variants to `quickstart`, parse timestamps, allow intentional pre-signup exposures.

## Gold Layer Metrics
Final lifecycle metrics assembled in `gold_user_metrics.sql`:

| Metric            | Definition (SQL)                                |
|-------------------|-------------------------------------------------|
| `activation`      | First `complete_quiz` event after signup        |
| `retained_7d`     | Any event between day 2-7 post-signup          |
| `started_trial`   | First `start_trial` event                       |
| `paid`            | First valid subscription after anomaly removal  |
| `trial_to_paid`   | Indicator: 1 if a trial starter converted to paid; 0 if not; `NULL` if no trial |
| `hours_to_activation` | Hours from signup to activation             |
| `hours_exposure_to_activation` | Hours from first exposure to activation |
| `event_depth_48h` | Count of all events in the first 48h post-signup   |
| `funnel_anomaly`  | `trial_without_activation` or `paid_without_activation` (else `NULL`) |

Monotonic funnel checks enforce `signups >= activation >= trials >= paid`.

## Experimental Exposure Groups
Mutually exclusive groups applied exactly as in SQL:
- `quickstart_and_trial`: `quickstart = 1` and `trial_campaign = 1`
- `quickstart_only`: `quickstart = 1` and `trial_campaign = 0`
- `trial_campaign_only`: `quickstart = 0` and `trial_campaign = 1`
- `control`: no QuickStart and no `promo_q3` trial exposure (may still have other standard campaigns)

Control validity is based on stakeholder confirmation that no other overlapping experiments were active; assignments are observational (no randomization enforced in SQL).


## Key Assumptions
- **Campaign & exposure**
  - `promo_q3` is the only experiment-related trial campaign; `new_user`, `retargeting`, and `summer_sale` are baseline marketing.
  - Trial campaign exposure uses any-touch: any `promo_q3` click (pre- or post-signup) sets `trial_campaign_exposed = 1`.
  - QuickStart exposure is binary (ever exposed); pre-signup exposures are treated as valid and kept.
  - Exposure groups are mutually exclusive from `quickstart_exposed` x `trial_campaign_exposed`.
  - Control is not "no marketing"; it only excludes QuickStart and `promo_q3` and may include other channels/campaigns.
  - Stakeholder-provided: no overlapping experiments or feature rollouts in the analysis period.
- **Attribution**
  - Deterministic order: user `utm_source` -> earliest pre-signup marketing click -> `unknown`.
  - Provenance stored in `utm_source_origin` in {`user_table`, `marketing_click_backfill`, `unknown`} to audit reliability.
  - User- vs click-level UTM mismatches are expected from multi-touch; the hierarchy enforces consistency.
- **Metrics & funnel**
  - Activation uses `complete_quiz` (only reliable onboarding milestone available).
  - Retention uses days 2-7 post-signup.
  - `trial_to_paid` is a user-level indicator for trial starters (aggregate downstream).
  - Funnel anomaly flags only unexpected paths: `trial_without_activation`, `paid_without_activation`; clean funnels remain `NULL`.

## Data Quality Audits
Major issues surfaced and mitigations:

| Issue                           | Impact                     | Mitigation                       |
|---------------------------------|----------------------------|----------------------------------|
| Missing `user_id` events        | ~17% of raw events         | Excluded                         |
| Events before signup            | 40-50% for some event types| Removed                          |
| Duplicate events                | 2,099 extra rows           | Deduplicated                     |
| UTM mismatches                  | ~64%                       | Resolved via attribution hierarchy |
| Subscription anomalies          | 375 rows                   | Excluded from metrics            |
| Unknown UTM source              | ~16%                       | Segmented separately             |
| Unlinked marketing clicks       | ~1-2%                      | Reported and excluded            |

Conclusion: Post-cleaning Gold data is fit for purpose without meaningful bias from observed issues.

## Validation Tests
Validation suite (Phase 5):
- Integrity: row-count parity between Gold/Silver, non-null required fields, no duplicate `user_id`, binary exposure checks.
- Metric validity: `activated`, `retained_7d`, `paid` are binary; no negative activation times; `event_depth` >= 0.
- Funnel: monotonic signup -> activation -> trial -> paid; anomaly distribution present across categories.

All tests passed or behaved as expected (minor funnel anomalies expected in real data).

## Limitations
- 16% unknown UTM reduces channel attribution precision.
- Funnel non-linearity (users skipping steps) persists.
- Control group validity relies on stakeholder confirmation.
- Activation proxy may miss alternative onboarding paths.
- Small sample sizes in some exposure x channel intersections.
- Upstream event ordering limits behavioral path resolution.

## Repository Map
- `bronze_layer/`: Raw ingestion logic.
- `silver_layer/`: Cleaning, parsing, deduplication, and attribution SQL.
- `gold_layer/`: Final Gold table with one row per user
- `dq_tests.sql`: Data quality and validation checks.
- `validation_tests.sql`: Additional integrity and funnel tests.




