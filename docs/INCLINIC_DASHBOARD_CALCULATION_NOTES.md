# InClinic Dashboard Calculation Notes

This note explains the current InClinic campaign dashboard calculations in business language. Use this as the reference document when discussing metric changes with a campaign brand manager or client.

Scope covered:

- Main campaign health cards.
- KPI tiles.
- Week filter and weekly trend chart.
- States requiring attention.
- Field Representative Insights and Excel export.
- Current, best, and benchmark collateral cards.
- Important interpretation caveats where two numbers should not be expected to match.

This document describes the current implementation in `dashboard/views.py`, `dashboard/templates/dashboard/overview.html`, `etl/pipelines/silver_transform.py`, and `etl/pipelines/gold_aggregations.py`.

---

## 1. Plain-English Data Flow

The dashboard does not read directly from MySQL source tables at page load. Source data is first processed by ETL into PostgreSQL reporting tables.

1. Source data comes from the InClinic source system.
2. Raw and bronze layers keep source-like records.
3. Silver layer standardizes doctor, campaign, field rep, collateral, share, and transaction data.
4. Gold layer creates campaign-specific reporting schemas.
5. The dashboard reads PostgreSQL silver/gold tables and renders the report.

Important source concepts:

- `sharing_management_sharelog`: records that a collateral/share message was sent.
- `sharing_management_collateraltransaction`: records doctor engagement with collateral, such as viewed, PDF saved, video progress, etc.
- `doctor_viewer_doctor`: doctor master/current doctor-to-field-rep mapping.
- `campaign_campaignfieldrep`: campaign-to-field-rep assignment list.
- `campaign_fieldrep`: master field rep details, including brand-supplied field rep ID and state.
- `collateral_management_campaigncollateral`: collateral schedule dates.

---

## 2. Campaign ID Matching

The dashboard accepts campaign IDs in URL form and then normalizes them for matching.

Normalization rule:

```text
lowercase + remove non-alphanumeric characters
```

Example:

```text
83ce7fc7-c965-433a-b2b9-717394abe3c1
83ce7fc7c965433ab2b9717394abe3c1
```

Both can resolve to the same campaign because the dashboard compares normalized IDs.

The report may also use campaign variants from:

- `gold_global.campaign_registry`
- `silver.map_brand_campaign_to_campaign`
- `bronze.campaign_management_campaign`

Brand-manager explanation:

> The dashboard treats dashed and non-dashed versions of the same campaign ID as the same campaign where possible.

---

## 3. Doctor Identity and Deduplication

Most metrics count unique doctors. A "unique doctor" is not always the source table row ID. The ETL creates a `doctor_identity_key`.

For collateral transactions:

```text
doctor_identity_key = MD5(
  doctor_unique_id if present,
  else normalized doctor phone number if present,
  else transaction row ID
)
```

For share logs:

```text
doctor_identity_key = MD5(
  doctor_identifier if present,
  else share log row ID
)
```

Business meaning:

> If the same doctor has multiple transaction rows, the dashboard tries to count that doctor once using doctor unique ID or phone number. If the source does not provide a stable doctor identifier, fallback row IDs can create less perfect matching.

Important caveat:

> If one source uses phone number and another source uses a different doctor identifier, the same real doctor may not always collapse into one technical key. This is a source-data matching limitation, not a manual dashboard edit.

---

## 4. Current Collateral and Schedule Selection

The dashboard chooses the "current collateral" from campaign collateral schedule data.

Priority order:

1. Collateral whose schedule includes today.
2. Past collateral, choosing the most recent applicable schedule.
3. Future collateral, choosing the nearest upcoming schedule.
4. Fallback records if schedule dates are missing.

Schedule dates come from:

- Collateral schedule start/end date when available.
- Campaign start/end date as fallback.

If multiple collaterals share the same primary schedule window, they are grouped as current collateral IDs.

Brand-manager explanation:

> The dashboard is designed to show the collateral that is currently active for the campaign. If no active collateral exists, it falls back to the closest sensible schedule.

---

## 5. Week Calculation

Weeks are not calendar months.

Current rule:

```text
Week 1 starts on the collateral/campaign schedule start date.
Each week is a 7-day bucket.
Weeks continue until today, or the schedule end date if earlier.
```

Example:

```text
Schedule: Apr 10, 2026 to Jun 10, 2026
Week 1: Apr 10 to Apr 16
Week 2: Apr 17 to Apr 23
Week 3: Apr 24 to Apr 30
...
```

Future weeks are not shown beyond today.

Active week marker:

```text
Week number with "*" = week has at least one activity event.
```

Activity event for week activation means at least one of:

- reached
- opened
- video >50%
- PDF download
- consumed

---

## 6. Event Definitions

### 6.1 Reached

In the silver layer, a doctor is considered reached if the system can find any first reachable event for that doctor/campaign/collateral.

Priority:

```text
share reached timestamp
else transaction reached timestamp
else opened timestamp
else video timestamp
else PDF timestamp
```

Transaction reached timestamp is:

```text
sent_at
else transaction_date
else created_at
```

Share reached timestamp is:

```text
share_timestamp
else created_at
```

Brand-manager explanation:

> Reached means the collateral/share activity reached the doctor record in the system. If the explicit sent timestamp is missing but a later engagement exists, the dashboard can still treat the doctor as reached because the doctor must have received access before engaging.

### 6.2 Opened

Opened means the doctor opened/viewed the collateral.

Opened timestamp is:

```text
first_viewed_at
else viewed_at
```

For field rep insights, a doctor is counted as viewed if:

```text
has_viewed is true
or opened timestamp exists
```

### 6.3 Video Viewed Greater Than 50%

The main KPI "Video Viewed" is the stricter video metric.

Video >50% is true if any of the following are true:

```text
video_view_gt_50 flag is true
or last_video_percentage >= 50
or video_watch_percentage >= 50
or video_gt_50_at timestamp exists
```

The event timestamp used is the first qualifying video timestamp after the above check.

Important distinction:

> The KPI tile says "Video Viewed (>50%)." Field Representative Insights says "Video Played." Those are not identical. The field-rep table uses a broader video-play rule.

### 6.4 Video Played in Field Representative Insights

Field Rep Insights counts a doctor as video played if any video activity exists, including partial video activity.

Field-rep video played is true if any of these exist:

```text
video >50 flag is true
or last_video_percentage > 0
or video_watch_percentage > 0
or video_lt_50_at exists
or video_gt_50_at exists
or video_100_at exists
```

Brand-manager explanation:

> The main video KPI is stricter because it measures meaningful video viewing above 50%. The field rep table is broader because it shows whether that rep generated any video play activity.

### 6.5 PDF Download / PDF Saved

Main KPI PDF download:

```text
downloaded_pdf flag is true
```

PDF first timestamp:

```text
viewed_last_page_at
else updated_at
```

Field Rep Insights PDF / Collateral Saved:

```text
downloaded_pdf flag is true
or PDF download event timestamp exists
```

Important caveat:

> The field-rep PDF metric is broader than the main KPI PDF tile. It may behave closer to "PDF/collateral saved or completed engagement event" depending on available transaction timestamps. If the client wants strict PDF downloads only, this field-rep rule should be changed.

### 6.6 Consumed

Consumed is used in health-score calculations.

Consumed means:

```text
video viewed >50%
or PDF downloaded
```

---

## 7. Campaign Doctor Denominator

The dashboard uses one campaign-level denominator called total doctors in campaign.

Current calculation:

```text
total doctors = greatest of:
  A. distinct assigned doctors found through campaign field-rep mappings,
  B. campaign_management_campaign.num_doctors,
  C. campaign_campaign.num_doctors_supported
```

If this result is available, it is used as the reporting denominator.

If it is not available, fallback is:

```text
sum of field-rep assigned doctor counts
```

Brand-manager explanation:

> The dashboard uses the best available campaign-level doctor count. For Apex, this can show as 3000 even if the sum of field-rep row assignments is higher, because the campaign denominator is intentionally a unique campaign-level number, not a sum of rep rows.

Important caveat:

> The field-rep row "Doctors Assigned" is currently calculated from doctor-to-rep mappings and can overlap across reps or include broader rep doctor mapping. Therefore, summing the field-rep rows can exceed the campaign-level total.

---

## 8. Main KPI Tiles

The visible KPI tiles are:

- Doctors Reached
- Doctors Opened
- Video Viewed
- PDF Downloads

### 8.1 Selected metric window

If a specific week is selected:

```text
KPI tiles use that selected week.
```

If "All Weeks" is selected:

```text
KPI tiles currently use the latest active week, not a cumulative all-week total.
```

Important caveat:

> "All Weeks" affects the weekly trend display, but the top KPI tiles currently show the latest active metric row. If the client expects cumulative all-weeks KPI tiles, this should be changed explicitly.

### 8.2 Doctors Reached

```text
Doctors Reached = unique doctors with reached/open/video/PDF activity in the selected week.
```

Percentage:

```text
Doctors Reached % = Doctors Reached / Total Doctors in Campaign * 100
```

Example from the current screenshot:

```text
1165 / 3000 * 100 = 38.8%
```

### 8.3 Doctors Opened

```text
Doctors Opened = unique doctors with opened timestamp in the selected week.
```

Percentage:

```text
Doctors Opened % = Doctors Opened / Doctors Reached * 100
```

Example:

```text
126 / 1165 * 100 = 10.8%
```

### 8.4 Video Viewed

```text
Video Viewed = unique doctors with video >50% timestamp in the selected week.
```

Percentage:

```text
Video Viewed % = Video Viewed / Doctors Opened * 100
```

Example:

```text
8 / 126 * 100 = 6.3%
```

### 8.5 PDF Downloads

```text
PDF Downloads = unique doctors with PDF download timestamp in the selected week.
```

Percentage:

```text
PDF Downloads % = PDF Downloads / Doctors Opened * 100
```

Example:

```text
35 / 126 * 100 = 27.8%
```

---

## 9. Campaign Health Score

Campaign Health is a score out of 100.

Formula:

```text
Campaign Health =
  50% * Reach Component
  + 25% * Open Component
  + 25% * Consumption Component
```

Where:

```text
Reach Component = min(Doctors Reached / Total Doctors, 100%)
Open Component = min(Doctors Opened / Total Doctors, 100%)
Consumption Component = min(Doctors Consumed / Total Doctors, 100%)
```

Doctors consumed:

```text
unique doctors with video >50% or PDF download
```

Important detail:

> Open and consumption components are compared against total doctors, not against reached or opened doctors. This keeps the health score anchored to full campaign scale.

### 9.1 Campaign Health vs Weekly Campaign Health

Campaign Health:

```text
uses summed metrics across active weeks
```

Weekly Campaign Health:

```text
uses the selected/latest week only
```

### 9.2 Health colors

```text
0 to <40 = red
40 to <60 = yellow
60 and above = green
```

### 9.3 Campaign WoW

Campaign WoW:

```text
current campaign health - previous campaign health using prior active weeks
```

If there are no previous active weeks:

```text
WoW = 0
```

### 9.4 Weekly WoW

Weekly WoW:

```text
current week health - previous week health
```

If there is no previous week:

```text
WoW = 0
```

---

## 10. Benchmark Labels

### 10.1 Campaign benchmark label

The dashboard reads the average campaign health score from:

```text
gold_global.benchmark_last_10_campaigns
```

Label:

```text
if current campaign health >= average benchmark health:
  Above Average
else:
  Below Average
```

### 10.2 Weekly benchmark label

Weekly benchmark label uses fixed thresholds:

```text
weekly health < 40 = Low
40 <= weekly health < 60 = Average
weekly health >= 60 = Good
```

---

## 11. States Requiring Attention

The state panel is meant to show weak states first.

### 11.1 State attribution

The dashboard tries to find the doctor's state in this priority order:

1. State already present in the gold fact row.
2. State from `silver.bridge_brand_campaign_doctor_base`.
3. State from `silver.dim_doctor`.
4. State from `silver.dim_field_rep`.
5. State from campaign field-rep roster using field rep aliases.
6. State from global field-rep master data.
7. `UNKNOWN`.

Field rep aliases include:

- internal field rep ID
- brand-supplied field rep ID
- auth email
- auth username
- local app user ID
- local field ID
- local email
- local username
- legacy field representative ID
- legacy field ID
- legacy email
- legacy Gmail
- legacy WhatsApp

### 11.2 State universe

States appear if they are found in:

- campaign field-rep roster, or
- enriched doctor/fact activity rows.

`UNKNOWN` is excluded from visible fact-derived states.

### 11.3 State reached count

```text
State Reached = unique doctors in that state with effective reached timestamp in the selected/latest week.
```

Effective reached timestamp:

```text
reached timestamp
else opened timestamp
else video timestamp
else PDF timestamp
```

### 11.4 State opened count

```text
State Opened = unique doctors in that state with opened timestamp in the selected/latest week.
```

### 11.5 State total

```text
State Total = unique doctors attributed to that state in the campaign gold fact table.
```

### 11.6 State percentages

```text
State Reached % = State Reached / State Total * 100
State Open % = State Opened / State Reached * 100
```

### 11.7 State health and label

State health currently gives open rate double weight:

```text
State Health =
  average(State Reached %, State Open %, State Open %)
```

Label:

```text
State Health < 40 = Low
40 <= State Health < 60 = Medium
State Health >= 60 = Good
```

Sort order:

```text
weakest states first
```

Important caveat:

> The state panel uses the selected/latest week window. It is not a lifetime campaign state summary unless the selected data window is changed.

---

## 12. Field Representative Insights

The Field Representative Insights modal and Excel download show one row per campaign field rep.

### 12.1 Field rep row selection

Field reps come from:

```text
bronze.campaign_campaignfieldrep
```

joined to:

```text
bronze.campaign_fieldrep
```

The row is grouped by the source campaign field rep ID to avoid duplicate display rows.

### 12.2 Field Rep ID shown

Display priority:

```text
brand_supplied_field_rep_id
else internal campaign field_rep_id
```

Brand-manager explanation:

> The report should show the brand-provided field rep ID where available.

### 12.3 Field rep name shown

Display priority:

```text
field rep full name
else brand supplied field rep ID
else internal field rep ID
else Unknown Field Rep
```

### 12.4 Doctors Assigned per field rep

Current calculation:

```text
Doctors Assigned =
  count distinct doctors in silver.dim_doctor
  where the doctor's rep key matches any known alias for the field rep
```

Aliases include internal ID, brand-supplied ID, local user ID, local field ID, email, username, legacy ID, legacy field ID, Gmail, and WhatsApp.

Important caveat:

> This is not currently guaranteed to be a strict campaign-roster-only doctor count. It is a doctor-to-rep mapping count. Therefore the sum of Doctors Assigned across field reps can exceed the campaign total doctor denominator.

### 12.5 Collateral Sent per field rep

Current calculation:

```text
Collateral Sent =
  count distinct doctor_identity_key
  from collateral transactions and share logs
  for the campaign/current collateral
  where the activity field rep key matches the field rep alias
```

Important caveat:

> This is a sum of per-rep unique doctor activity. A doctor can appear under more than one field rep if source activity or reassignment differs. Therefore this sum should not be expected to equal the global Doctors Reached tile.

### 12.6 Viewed per field rep

Current calculation:

```text
Viewed =
  count distinct doctors from collateral transactions
  where has_viewed is true
  or opened timestamp exists
```

### 12.7 Video Played per field rep

Current calculation:

```text
Video Played =
  count distinct doctors with any video activity
```

This is broader than the main KPI video >50%.

### 12.8 PDF / Collateral Saved per field rep

Current calculation:

```text
PDF / Collateral Saved =
  count distinct doctors where downloaded_pdf is true
  or a PDF download event timestamp exists
```

Important caveat:

> This field-rep metric is broader than the main PDF Downloads KPI. If the brand manager wants only strict source `downloaded_pdf=true`, this rule should be changed.

### 12.9 Assignment issue warning

The dashboard can still calculate a hidden assignment note.

Issue condition:

```text
no assigned doctors but activity exists
or collateral sent > doctors assigned
```

The detailed "Data Note" column is hidden from the modal, but the warning count can still appear above the table.

---

## 13. Field Representative Summary Tile

The Field Representative Insights tile shows:

```text
Field Reps = number of field rep rows
Doctors Assigned = campaign-level total doctor denominator
Sent = sum of field-rep Collateral Sent rows
Viewed = sum of field-rep Viewed rows
Video = sum of field-rep Video Played rows
PDF = sum of field-rep PDF / Collateral Saved rows
```

Important interpretation:

> Doctors Assigned on the tile is campaign-level. Sent/Viewed/Video/PDF on the tile are field-rep-row sums. These are different aggregation types.

Example from current Apex export:

```text
Field Rep rows = 230
Campaign denominator shown on tile = 3000
Sum of field-rep Doctors Assigned rows = 6147
Sum Collateral Sent = 2387
Sum Viewed = 252
Sum Video Played = 15
Sum PDF / Collateral Saved = 252
```

Why this can happen:

```text
3000 = campaign-level doctor denominator
6147 = sum of per-rep doctor mapping counts
```

These should not be treated as the same metric.

Client decision point:

> If the client expects the field-rep summary tile and Excel totals to reconcile exactly with the KPI tiles, the field-rep table should be rebuilt on a campaign-specific doctor roster and a single owner rep per doctor.

---

## 14. Excel Download

The "Download Excel" button exports the visible Field Representative Insights table from the browser as an HTML `.xls` file.

The app exports these columns:

- Field Rep ID
- Field Representative
- Doctors Assigned
- Collateral Sent
- Viewed
- Video Played
- PDF / Collateral Saved

The app does not generate sum formulas in the export.

If the downloaded file contains extra columns such as:

- Sum Doctors Assigned
- Sum Collateral Sent
- Sum Viewed

those are not generated by the dashboard code. They were added after export or by the spreadsheet tool/user workflow.

---

## 15. Weekly Trend Chart

The chart uses week rows.

If a week filter is selected:

```text
chart shows only that selected week
```

If All Weeks is selected:

```text
chart shows only active weeks with at least one metric event
```

Series formulas:

```text
Doctors Reached % = Doctors Reached / Total Doctors in Campaign * 100
Doctors Opened % = Doctors Opened / Total Doctors in Campaign * 100
PDF Downloads % = PDF Downloads / Total Doctors in Campaign * 100
Video Viewed % = Video Viewed / Total Doctors in Campaign * 100
```

Important caveat:

> The chart percentages use total campaign doctors as denominator for every series. The KPI tiles use different denominators for opened, video, and PDF. Therefore chart percentages and KPI percentages are not expected to match.

---

## 16. Current, Best, and Benchmark Cards

### 16.1 Current collateral card

Uses the same selected/latest week values as the KPI tiles.

Shows:

- Doctors Reached and reach percentage.
- Doctors Opened and open percentage.
- Video Viewed >50% and video percentage.
- PDF Downloads and PDF percentage.

### 16.2 Best week card

The dashboard selects the active week with the highest health score.

Best week selection formula:

```text
highest Engagement Health Score among active weeks
```

The same percentage rules are used:

```text
Reached % = reached / total doctors
Opened % = opened / reached
Video % = video / opened
PDF % = PDF / opened
```

### 16.3 Benchmark card

The dashboard looks at recent campaigns from `gold_global.campaign_health_history` and calculates a best benchmark campaign.

Benchmark metric formulas:

```text
Reached % = reached / total doctors
Opened % = opened / reached
Video % = video / opened
PDF % = PDF / opened
Benchmark Health =
  50% * reached / total doctors
  + 25% * opened / total doctors
  + 25% * max(video, PDF) / total doctors
```

Important caveat:

> Benchmark logic is global across recent campaigns. It is not brand-specific unless the source benchmark table is changed to filter by brand/client.

---

## 17. Action Recommendation Logic

The code still computes an action recommendation, although the current template does not display it as a main visible tile.

The weakest area is selected from:

```text
Open rate
Consumption rate
Reach rate
```

If open rate is weakest:

```text
Primary issue = Low Open Rate
Owner = Field Team Lead
```

If consumption is weakest:

```text
Primary issue = Low Consumption Conversion
Owner = Content + Field Team
```

If reach is weakest:

```text
Primary issue = Low Reach Coverage
Owner = Field Team Lead
```

---

## 18. Numbers That Should Match

These should match:

```text
Field Rep tile Sent = Excel sum of Collateral Sent
Field Rep tile Viewed = Excel sum of Viewed
Field Rep tile Video = Excel sum of Video Played
Field Rep tile PDF = Excel sum of PDF / Collateral Saved
KPI Doctors Reached % = KPI Doctors Reached / Campaign Total Doctors
KPI Doctors Opened % = KPI Doctors Opened / KPI Doctors Reached
KPI Video % = KPI Video / KPI Doctors Opened
KPI PDF % = KPI PDF / KPI Doctors Opened
```

These should not automatically be expected to match:

```text
Field Rep Sent sum vs KPI Doctors Reached
Field Rep Viewed sum vs KPI Doctors Opened
Field Rep PDF sum vs KPI PDF Downloads
Sum of Field Rep Doctors Assigned vs Campaign Total Doctors
Weekly chart percentages vs KPI conversion percentages
State percentages vs campaign-level percentages
```

Reason:

> The first group compares like-for-like calculations. The second group compares different aggregation levels or different business definitions.

---

## 19. Current Known Interpretation Caveats

These are not necessarily code errors, but they should be discussed before promising numbers to a client.

1. Field Rep Doctors Assigned can sum higher than campaign total doctors because field-rep assigned counts are not strictly campaign-roster-only.
2. Field Rep Sent/Viewed/Video/PDF are per-rep sums and may not reconcile to global unique KPI tiles.
3. Field Rep Video Played is broader than KPI Video Viewed >50%.
4. Field Rep PDF / Collateral Saved is broader than KPI PDF Downloads.
5. "All Weeks" KPI cards currently show the latest active metric row, while Campaign Health uses active-week rollup.
6. Weekly trend percentages use total campaign doctors as denominator for all series.
7. State panel is latest-week based, not full-campaign lifetime unless the week/window logic is changed.
8. Benchmark is global recent-campaign based, not brand-specific.

---

## 20. Recommended Client Decision Points

If a brand manager requests changes, decide these rules explicitly before implementation:

1. Should "All Weeks" KPI tiles show cumulative all-week totals or latest active week?
2. Should field-rep Doctors Assigned be strict campaign roster only?
3. Should each doctor have exactly one owner field rep for reporting?
4. Should Field Rep Sent reconcile exactly with global Doctors Reached?
5. Should Field Rep PDF count only `downloaded_pdf=true`?
6. Should Field Rep Video count any play or only >50% view?
7. Should state metrics be weekly, all-weeks cumulative, or lifetime campaign?
8. Should trend chart conversion rates use total doctors or funnel denominators?
9. Should benchmark be global, brand-specific, therapy-specific, or campaign-type-specific?

Recommended brand-manager language:

> Before changing numbers, first confirm whether the client wants campaign-level unique counts, field-rep-level ownership counts, or source-event counts. These three views answer different business questions and should not be mixed without a clear rule.

