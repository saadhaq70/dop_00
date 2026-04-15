# Pipeline Audit: IDSP Scraping → ML-Ready Dataset

## Pipeline Flow

```
idsp_scraper.py  →  idsp_outbreaks_2013_2025.csv  →  preprocess.py  →  idsp_disease_data.csv  →  build_dataset.py  →  ml_ready_idsp.csv
```

---

## Stage 1 — `idsp_scraper.py` (Scraping)

### ✅ Working
- Polite crawling (1.2s delay), timeout, and UA headers set
- Dual extraction strategy: table parser → text fallback
- Download log + URL manifest saved

### 🔴 Critical Issues

#### 1. **Massive data gap: 2024 is nearly empty**
Only **1 week** (week 4) of 2024 has a direct PDF link — all other 51 weeks
point to Google Drive, which the scraper explicitly **skips**.
The yearly summary confirms: `2024 → only 5 rows, 156 cases`.
This creates a massive temporal hole right before the test split cutoff (2023-01-01).

#### 2. **~549 junk rows parsed as real outbreak data**
The PDF header text ("Through Integrated Disease Surveillance Project (IDSP)",
"1st Week (31 December 2012…)") is being parsed as `state` and `disease`
values. These rows have no `cases` or `deaths` and will pollute aggregations.

```
state  ← "Through Integrated Disease Surveillance Project (Idsp)"
disease ← "1St Week ( 31 December 2012 To 6 January 2013)"
```

#### 3. **Disease column polluted with date strings (977 "unique" diseases in 2013)**
Because the PDF header rows are parsed, the `disease` column contains week
date strings like `"10Th Week (02Nd March To 08Th March) 2015"`, inflating
unique disease counts wildly. 2013 shows **977 diseases**, 2014 shows **668**.
Real unique diseases across the whole dataset should be ~35–40.

#### 4. **`report_week` column is 99.9% null** (`12,253 / 12,264` rows)
This column is extracted via `fc("week")` but almost no PDF table has a
"week" header column — the week info is in `year`/`week` metadata already.
It adds no value but wastes a column.

#### 5. **`date_reporting` is ~74% null** (`9,073 / 12,264`)
Though parsed, it's a raw string (e.g. `"02/1/13"`) with no standardization.
It is never parsed into a proper date type, making it unusable downstream.

#### 6. **Text fallback is error-prone**
`extract_text_fallback` grabs the **first number** on a line as `cases` and
**second** as `deaths` — with no positional context. For a line like
`"Andhra Pradesh 14 districts Dengue 200 cases reported"`, it would set
`cases=14`, `deaths=200`, both wrong.

#### 7. **Missing weeks not flagged**
2023 is missing weeks 26–32, 39–41, 43 (gaps in the URL table with no
explanation). The log marks them as `ok` because they never appear in the
URL table — so gaps are silently ignored rather than flagged.

---

## Stage 2 — `preprocess.py` (Cleaning & Aggregation)

### ✅ Working
- Week→date conversion using `%Y-%W-%w`
- State-level aggregation (`groupby date, state, disease`)
- Column rename to `total_cases`

### 🔴 Critical Issues

#### 8. **Junk rows from the scraper survive `dropna`**
`preprocess.py` only drops rows where `state`, `disease`, `year`, or `week`
are NaN. The ~549 junk rows have all four columns populated (with header
text), so they **pass through** and get aggregated as if they were real
outbreaks.

#### 9. **Hardcoded relative paths will break when run from any other directory**
```python
INPUT_FILE  = "idsp_output/idsp_outbreaks_2013_2025.csv"   # relative
OUTPUT_FILE = "data/processed/idsp_disease_data.csv"        # relative
```
If run from the project root instead of `data/`, both paths fail silently.
`build_dataset.py` (correctly) uses `Path(__file__).resolve()` — `preprocess.py` should too.

#### 10. **`deaths` column is silently dropped**
The aggregation only sums `cases` — `deaths` is never forwarded to the
processed file. Downstream models have no mortality signal at all.

#### 11. **Week 53 edge case (2020) is not validated**
ISO week numbering has 53 weeks in some years. `%Y-%W-%w` (stdlib week)
and ISO weeks can differ. Week 53 of 2020 with `%Y-%W-%w` correctly gives
`2021-01-04`, but this is not explicitly validated and could silently shift
dates by a week for late-year entries.

---

## Stage 3 — `build_dataset.py` (Feature Engineering, ML Prep)

### ✅ Working
- Temporal train/test split at 2023-01-01 (no leakage)
- Leakage-safe rolling features (shifts by 1 before rolling)
- Feature schema saved to JSON
- `dropna` on lag/rolling/target cols to remove init gaps

### 🔴 Critical Issues

#### 12. **`date` column is dropped from ML dataset but referenced in split**
`apply_feature_engineering` → `encode_categorical_features` calls
`pd.get_dummies(df, columns=['state', 'disease'])` which drops `state`
and `disease` but **keeps** `date`. However `temporal_train_test_split`
references `df['date']` **after** `finalize_schema` runs — which reorders
columns but doesn't explicitly ensure `date` is present. If `date` ever
gets dropped (e.g. via a future column-cleaning step), the split silently
breaks.

#### 13. **`build_target` runs `dropna` inside, then `handle_missing_values` runs another `dropna` — double-drop**
`build_target` already calls `df.dropna(subset=['target'])`, removing the
last row of each `(state, disease)` group. Then `handle_missing_values`
calls `dropna` again on the same plus lag/rolling cols. The print statement
reports an inflated "initialization rows dropped" count because it includes
rows already removed by `build_target`.

#### 14. **Rolling features are computed on `cases_shifted_1` but parameters say `target_col='total_cases'`**
In `build_dataset.py line 70`:
```python
df = add_rolling_features(df, group_cols=group_cols, target_col='cases_shifted_1')
```
The `target_col` argument is correctly passed as `'cases_shifted_1'`, so
this works — but the default signature of `add_rolling_features` in
`features.py` says `target_col='total_cases'`, which would cause leakage
if called with defaults. This is a latent bug waiting to surface.

#### 15. **One-hot encoding of `state`+`disease` is done before lag/rolling → column count explosion inflates memory**
`encode_categorical_features` is called after lags/rolling, but `state` and
`disease` are used as `group_cols` inside `add_lag_features` and
`add_rolling_features` before encoding. This is logically correct, but the
OHE step in `apply_feature_engineering` (line 74) encodes immediately —
after which `build_target` (line 77) tries `groupby(group_cols)` on
**already-encoded** columns (`['state', 'disease']` no longer exist).
This will throw a **`KeyError`** at runtime.

#### 16. **`build_target` groupby will fail after OHE removes `state`/`disease`**
This is the immediate crash point: after `encode_categorical_features`,
the columns `'state'` and `'disease'` are gone (replaced by dummies).
`build_target` then calls:
```python
df.groupby(['state', 'disease'])['total_cases'].shift(-1)
```
→ **`KeyError: 'state'`**

---

## Stage 4 — `features.py`

### 🔴 Issues

#### 17. **`min_periods=1` on rolling std hides data scarcity**
`rolling_std_4` uses `min_periods=1`, which means early rows (only 1–3
observations) will return 0 std dev — masking data sparsity and producing
over-confident features. The `fillna(0)` doubles down on this.

#### 18. **No cyclical encoding for `month` or `week_of_year`**
`month` (1–12) and `week_of_year` (1–52) are treated as linear integers.
A model will see month 12→1 as a large jump, not a cyclical boundary.
Standard fix: `sin/cos` transforms.

#### 19. **`is_monsoon` hardcoded for pan-India; differs by region**
Monsoon months for Kerala (June) vs. Northeast India (April) are different.
A single binary `is_monsoon` flag loses regional granularity, especially
after state info is OHE'd away from the original columns.

---

## Summary Table

| # | Stage | Severity | Issue |
|---|-------|----------|-------|
| 1 | Scraper | 🔴 Critical | 2024 entirely missing (Google Drive skipped) |
| 2 | Scraper | 🔴 Critical | ~549 PDF header rows parsed as outbreak data |
| 3 | Scraper | 🔴 Critical | Disease col polluted with date strings |
| 4 | Scraper | 🟡 Medium | `report_week` column 99.9% null, useless |
| 5 | Scraper | 🟡 Medium | `date_reporting` 74% null, never parsed |
| 6 | Scraper | 🟡 Medium | Text fallback assigns numbers by position, not meaning |
| 7 | Scraper | 🟡 Medium | Missing weeks silently undetected |
| 8 | Preprocess | 🔴 Critical | Junk rows survive `dropna`, enter aggregation |
| 9 | Preprocess | 🟡 Medium | Hardcoded relative paths |
| 10 | Preprocess | 🟡 Medium | `deaths` column silently dropped |
| 11 | Preprocess | 🟠 Low | Week-53 ISO edge case unvalidated |
| 12 | Build | 🟡 Medium | `date` col not guaranteed present after schema finalization |
| 13 | Build | 🟠 Low | Double `dropna` inflates "dropped rows" count |
| 14 | Build | 🟡 Medium | Latent leakage bug in `add_rolling_features` default arg |
| **15** | **Build** | **🔴 Critical** | **OHE removes `state`/`disease` before `build_target` groupby** |
| **16** | **Build** | **🔴 Critical** | **`KeyError: 'state'` crash at runtime** |
| 17 | Features | 🟡 Medium | `min_periods=1` + `fillna(0)` masks data scarcity on std |
| 18 | Features | 🟡 Medium | `month`/`week_of_year` not cyclically encoded |
| 19 | Features | 🟠 Low | `is_monsoon` is pan-India, not region-aware |




<!-- 
idsp_scraper.py  →  idsp_outbreaks_2013_2025.csv  →  preprocess.py  →  idsp_disease_data.csv  →  build_dataset.py  →  ml_ready_idsp.csv -->
