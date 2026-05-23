# InClinic Dashboard Calculation Notes

This note explains the dashboard calculations in plain business language. It is meant for campaign brand managers and client discussions, not for database or engineering review.

Use this note as the baseline before changing any dashboard number. If a client wants a different number, first confirm which definition below they want changed.

---

## 1. Core Counting Principle

Most dashboard numbers count unique doctors, not raw activity rows.

Simple rule:

```text
One doctor should normally count once for one metric in one reporting window.
```

Example:

```text
If the same doctor opens the same collateral five times, the doctor should count as one opened doctor, not five opens.
```

Important limitation:

```text
If the source system identifies the same real doctor differently in different places, perfect deduplication may not always be possible.
```

---

## 2. Campaign Doctor Total

This is the denominator used for the main campaign percentages.

Simple meaning:

```text
Campaign Doctor Total = total doctors expected for the campaign.
```

Current logic:

```text
Use the best available campaign-level doctor count.
If a declared campaign doctor count exists, use it.
If a supported doctor count exists and is higher, use it.
If roster-matched unique doctors are higher, use that higher count.
```

Why this matters:

```text
The campaign total is not calculated by adding every field rep row.
```

Example:

```text
Campaign Doctor Total shown on tile = 3000
Sum of doctors assigned across field reps = 6147
```

These can both happen because:

```text
3000 is the campaign-level denominator.
6147 is a sum of field-rep-level doctor mappings.
```

Brand-manager explanation:

```text
The campaign total answers: "How many doctors are in this campaign overall?"
The field rep sum answers: "How many doctor mappings are attached to all reps?"
Those are related, but they are not the same business question.
```

---

## 3. Current Collateral Selection

The dashboard tries to show the currently relevant collateral.

Current priority:

```text
1. Collateral active today.
2. If none is active, the most recent past collateral.
3. If none is past, the nearest future collateral.
4. If dates are incomplete, use the best available campaign schedule.
```

If more than one collateral belongs to the same current schedule window, they can be grouped together.

Brand-manager explanation:

```text
The report focuses on the collateral that should matter right now for the campaign.
```

---

## 4. Week Logic

Weeks are campaign weeks, not calendar-month weeks.

Current rule:

```text
Week 1 starts on the scheduled collateral/campaign start date.
Each week is seven days.
Future weeks are not shown beyond today.
```

Example:

```text
Schedule starts Apr 10.
Week 1 = Apr 10 to Apr 16.
Week 2 = Apr 17 to Apr 23.
Week 3 = Apr 24 to Apr 30.
```

Week with activity:

```text
A week is marked active if at least one doctor has reach, open, video, PDF, or consumption activity in that week.
```

---

## 5. Reach

Simple meaning:

```text
Reached = unique doctors who received or had any traceable access/activity for the collateral.
```

A doctor is considered reached if any of the following exists:

```text
1. Share/send event.
2. Transaction/send event.
3. Open event.
4. Video event.
5. PDF event.
```

Why open/video/PDF can imply reach:

```text
If a doctor opened, watched, or downloaded, the doctor must have received access somehow.
```

Main formula:

```text
Doctors Reached % = Doctors Reached / Campaign Doctor Total * 100
```

Example:

```text
1165 reached doctors / 3000 campaign doctors = 38.8%
```

---

## 6. Opened

Simple meaning:

```text
Opened = unique doctors who opened or viewed the collateral.
```

Main formula:

```text
Doctors Opened % = Doctors Opened / Doctors Reached * 100
```

Example:

```text
126 opened / 1165 reached = 10.8%
```

Why denominator is reached, not campaign total:

```text
Open rate is a funnel conversion from reached doctors to opened doctors.
```

---

## 7. Video Viewed

The main KPI tile uses the strict video metric.

Simple meaning:

```text
Video Viewed = unique doctors who viewed more than 50% of the video.
```

Main formula:

```text
Video Viewed % = Video Viewed / Doctors Opened * 100
```

Example:

```text
8 video viewers / 126 opened doctors = 6.3%
```

Why denominator is opened:

```text
Video viewing is a conversion after opening the collateral.
```

---

## 8. PDF Downloads

Simple meaning:

```text
PDF Downloads = unique doctors who downloaded or saved the PDF/collateral according to the tracked PDF event.
```

Main formula:

```text
PDF Downloads % = PDF Downloads / Doctors Opened * 100
```

Example:

```text
35 PDF downloads / 126 opened doctors = 27.8%
```

Why denominator is opened:

```text
PDF download is a conversion after opening the collateral.
```

---

## 9. Consumed

Consumed is used inside health-score calculations.

Simple meaning:

```text
Consumed = unique doctors who either watched more than 50% of video or downloaded/saved the PDF.
```

Formula:

```text
Consumed Doctors = unique doctors with video >50% OR PDF download.
```

---

## 10. Campaign Health Score

Simple meaning:

```text
Campaign Health is a weighted score out of 100.
It combines reach, opening, and consumption.
```

Formula:

```text
Campaign Health =
  50% weight for reach
  + 25% weight for opened
  + 25% weight for consumed
```

Expanded formula:

```text
Campaign Health =
  0.50 * (Doctors Reached / Campaign Doctor Total)
  + 0.25 * (Doctors Opened / Campaign Doctor Total)
  + 0.25 * (Doctors Consumed / Campaign Doctor Total)
```

Then multiply by 100.

Why opened and consumed use campaign total here:

```text
Health score measures total campaign impact, not only funnel conversion.
```

Color rule:

```text
Below 40 = red.
40 to below 60 = yellow.
60 and above = green.
```

---

## 11. Campaign Health vs Weekly Campaign Health

Campaign Health:

```text
Uses active campaign weeks together.
It answers: "How is the campaign doing overall so far?"
```

Weekly Campaign Health:

```text
Uses the selected/latest week.
It answers: "How did the campaign perform in this week?"
```

Week-over-week movement:

```text
WoW = current score - previous comparable score.
```

---

## 12. All Weeks Behavior

Current behavior:

```text
When "All Weeks" is selected, the trend chart shows active weeks.
The top KPI tiles currently show the latest active metric row, not a cumulative all-weeks total.
```

Important client decision:

```text
If the client expects All Weeks to mean cumulative totals, this should be changed explicitly.
```

---

## 13. Weekly Trend Chart

The weekly chart is a week-by-week percentage chart.

Current chart formulas:

```text
Doctors Reached % = doctors reached in that week / campaign doctor total * 100.
Doctors Opened % = doctors opened in that week / campaign doctor total * 100.
Video Viewed % = video >50% doctors in that week / campaign doctor total * 100.
PDF Downloads % = PDF download doctors in that week / campaign doctor total * 100.
```

Important difference from KPI tiles:

```text
The chart uses campaign doctor total for all bars.
The KPI tiles use funnel denominators for opened, video, and PDF.
```

So these are not expected to match exactly:

```text
Chart opened percentage vs KPI opened percentage.
Chart video percentage vs KPI video percentage.
Chart PDF percentage vs KPI PDF percentage.
```

---

## 14. States Requiring Attention

Simple meaning:

```text
The state card identifies states where reach/open performance is weaker.
```

State assignment logic:

```text
First use doctor state if available.
If doctor state is unavailable or marked unknown, use campaign/field-rep state mapping.
If that is unavailable, use field-rep master state mapping.
If no valid state is found, keep it as UNKNOWN and do not show it as a normal state.
```

Important fix rule:

```text
Blank, null, none, and unknown are not treated as real states.
They are skipped so the dashboard can try the next fallback.
```

State reached:

```text
State Reached = unique doctors in that state with reach activity in the selected/latest week.
```

State opened:

```text
State Opened = unique doctors in that state with open activity in the selected/latest week.
```

State total:

```text
State Total = unique doctors attributed to that state for the campaign.
```

State formulas:

```text
State Reached % = State Reached / State Total * 100.
State Open % = State Opened / State Reached * 100.
```

State health:

```text
State Health = average of:
  State Reached %
  State Open %
  State Open %
```

Open rate is intentionally counted twice in this state health shortcut.

State label:

```text
Below 40 = Low.
40 to below 60 = Medium.
60 and above = Good.
```

Important caveat:

```text
The state list is based on the selected/latest week window, not automatically full campaign lifetime.
```

---

## 15. Field Representative Insights

Simple meaning:

```text
Field Representative Insights show how campaign activity is distributed by field rep.
```

One row means:

```text
One campaign field rep.
```

The report should show:

```text
Brand-provided field rep ID where available.
Field rep name where available.
Assigned doctors.
Collateral sent.
Viewed.
Video played.
PDF/collateral saved.
```

### 15.1 Doctors Assigned

Simple meaning:

```text
Doctors Assigned = unique doctors mapped to that field rep.
```

Important caveat:

```text
This currently uses doctor-to-rep mapping, not a perfectly strict campaign-only roster.
Because of that, summing Doctors Assigned across all reps can be higher than campaign doctor total.
```

Example:

```text
Campaign doctor total = 3000.
Sum of all field-rep assigned doctors = 6147.
```

This does not automatically mean 6147 doctors are in the campaign.

### 15.2 Collateral Sent

Simple meaning:

```text
Collateral Sent = unique doctors for whom that field rep has send/share activity.
```

Important caveat:

```text
This is a field-rep-level sum.
It does not have to equal global Doctors Reached.
```

Reason:

```text
The same doctor can appear under more than one rep if the source activity or reassignment differs.
```

### 15.3 Viewed

Simple meaning:

```text
Viewed = unique doctors under that field rep who viewed/opened the collateral.
```

### 15.4 Video Played

Simple meaning:

```text
Video Played = unique doctors under that field rep who had any video activity.
```

Important distinction:

```text
Field-rep Video Played is broader than the main KPI Video Viewed >50%.
```

### 15.5 PDF / Collateral Saved

Simple meaning:

```text
PDF / Collateral Saved = unique doctors under that field rep with PDF/collateral save or download activity.
```

Important distinction:

```text
This can be broader than the main PDF Downloads KPI.
```

---

## 16. Field Representative Summary Tile

The Field Representative tile combines two types of numbers.

Field Reps:

```text
Number of field rep rows.
```

Doctors Assigned:

```text
Campaign-level doctor total.
```

Sent, Viewed, Video, PDF:

```text
Sum of the field-rep rows.
```

Important explanation:

```text
Doctors Assigned on the tile is campaign-level.
Sent/Viewed/Video/PDF on the tile are field-rep-level sums.
They should not be forced to reconcile unless the business rule is changed.
```

Example:

```text
Tile doctors assigned = 3000.
Excel sum of field-rep assigned doctors = 6147.
Tile sent = Excel sum of field-rep sent.
```

---

## 17. Excel Download

Simple meaning:

```text
The Excel download exports the visible Field Representative Insights list.
```

Current exported values:

```text
Field Rep ID.
Field Representative.
Doctors Assigned.
Collateral Sent.
Viewed.
Video Played.
PDF / Collateral Saved.
```

The dashboard itself does not add separate sum formula columns.

If extra sum fields appear after download:

```text
They were added manually or by the spreadsheet software/workflow after export.
```

---

## 18. Current Collateral, Best Week, and Benchmark

Current collateral card:

```text
Shows the same selected/latest week metrics as the KPI tiles.
```

Best week card:

```text
Finds the active week with the best health score.
```

Benchmark card:

```text
Uses recent campaign history as a global benchmark.
It is not brand-specific unless the benchmark rule is changed.
```

Benchmark health:

```text
50% reach
+ 25% opened
+ 25% best of video or PDF consumption
```

---

## 19. Numbers That Should Match

These should match:

```text
Doctors Reached % = Doctors Reached / Campaign Doctor Total.
Doctors Opened % = Doctors Opened / Doctors Reached.
Video Viewed % = Video Viewed / Doctors Opened.
PDF Downloads % = PDF Downloads / Doctors Opened.
Field Rep tile Sent = Excel sum of Collateral Sent.
Field Rep tile Viewed = Excel sum of Viewed.
Field Rep tile Video = Excel sum of Video Played.
Field Rep tile PDF = Excel sum of PDF / Collateral Saved.
```

These should not automatically be expected to match:

```text
Campaign Doctor Total vs sum of field-rep assigned doctors.
KPI Doctors Reached vs field-rep Sent sum.
KPI Doctors Opened vs field-rep Viewed sum.
KPI Video Viewed >50% vs field-rep Video Played.
KPI PDF Downloads vs field-rep PDF / Collateral Saved.
KPI percentages vs weekly chart percentages.
Campaign state percentages vs overall campaign percentages.
```

Reason:

```text
They answer different business questions and use different denominators or grouping levels.
```

---

## 20. Client Decision Points Before Changing Metrics

Before implementing a requested metric change, confirm these points:

1. Should "All Weeks" mean latest active week or cumulative campaign-to-date?
2. Should field-rep assigned doctors be campaign-roster-only?
3. Should every doctor have one final owner field rep for reporting?
4. Should field-rep totals reconcile exactly with global KPI tiles?
5. Should video count any play or only more than 50% watched?
6. Should PDF count only confirmed downloads or broader saved/completed activity?
7. Should state metrics be weekly, all-weeks cumulative, or campaign lifetime?
8. Should benchmark be global, brand-specific, or therapy/category-specific?

Recommended brand-manager language:

```text
First decide whether the client wants campaign-level unique counts,
field-rep-level ownership counts, or source activity counts.
Those are different views and should not be mixed without a clear rule.
```
