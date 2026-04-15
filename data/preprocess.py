"""
preprocess.py
=============
Transforms raw IDSP scraper output into a clean, ML-ready CSV.

Steps:
  1. Filter junk rows (PDF header text parsed as state/disease)
  2. Standardize & normalize state names → 36 canonical entries
  3. Standardize & normalize disease names → ~40 canonical entries
  4. Convert Year/Week → Calendar Date (Monday of reporting week)
  5. Aggregate district-level rows to state-level sums (cases + deaths)
  6. Export to data/processed/idsp_disease_data.csv
"""

import re
import pandas as pd
from pathlib import Path

# ─── Paths ────────────────────────────────────────────────────────────────────
SCRIPT_DIR  = Path(__file__).resolve().parent
INPUT_FILE  = SCRIPT_DIR / "idsp_output" / "idsp_outbreaks_2013_2025.csv"
OUTPUT_FILE = SCRIPT_DIR.parent / "data" / "processed" / "idsp_disease_data.csv"

# ─── Junk detection ───────────────────────────────────────────────────────────
JUNK_PATTERN = re.compile(
    r"integrated\s+disease|surveillance|idsp|week\s*\(|"
    r"\d{1,2}\s*(st|nd|rd|th)\s+week|"
    r"january|february|march|april|may|june|july|august|"
    r"september|october|november|december",
    re.IGNORECASE,
)

# ─── State normalization map ───────────────────────────────────────────────────
# Maps every observed variant → canonical name.
STATE_MAP = {
    # Andaman & Nicobar Islands
    "Andaman & Nicobar Islands":                        "Andaman And Nicobar Islands",
    "Andaman And Nicobar Island":                       "Andaman And Nicobar Islands",
    "Andaman And Nicobar Islands":                      "Andaman And Nicobar Islands",
    # Andhra Pradesh
    "Andhra":                                           "Andhra Pradesh",
    "Andhra Pradesh":                                   "Andhra Pradesh",
    # Arunachal Pradesh
    "Arunachal Pradesh":                                "Arunachal Pradesh",
    "Arunachala Pradesh":                               "Arunachal Pradesh",
    # Assam
    "Assam":                                            "Assam",
    # Bihar
    "Bihar":                                            "Bihar",
    # Chandigarh
    "Chandigarh":                                       "Chandigarh",
    # Chhattisgarh
    "Chhattisgarh":                                     "Chhattisgarh",
    "Chhattisgarh (Ihip)":                              "Chhattisgarh",
    # Dadra & Nagar Haveli and Daman & Diu (merged UT since 2020)
    "Dadra & Nagar Haveli":                             "Dadra And Nagar Haveli And Daman And Diu",
    "Dadra And Nagar Haveli":                           "Dadra And Nagar Haveli And Daman And Diu",
    "Dadra And Nagar Haveli And Daman And Diu":         "Dadra And Nagar Haveli And Daman And Diu",
    "The Dadra And Nagar Haveli And Daman And Diu":     "Dadra And Nagar Haveli And Daman And Diu",
    "D&N Haveli And Daman And Diu":                     "Dadra And Nagar Haveli And Daman And Diu",
    "Daman & Diu":                                      "Dadra And Nagar Haveli And Daman And Diu",
    "Daman And Diu":                                    "Dadra And Nagar Haveli And Daman And Diu",
    # Delhi
    "Delhi":                                            "Delhi",
    # Goa
    "Goa":                                              "Goa",
    # Gujarat
    "Gujarat":                                          "Gujarat",
    "Gujarat (Ihip)":                                   "Gujarat",
    "Gujarat Gujarat":                                  "Gujarat",
    # Haryana
    "Haryana":                                          "Haryana",
    # Himachal Pradesh
    "Himachal":                                         "Himachal Pradesh",
    "Himachal Pradesh":                                 "Himachal Pradesh",
    # Jammu & Kashmir (and Ladakh — split in 2019)
    "J&K":                                              "Jammu And Kashmir",
    "Jammu & Kashmir":                                  "Jammu And Kashmir",
    "Jammu And Kashmir":                                "Jammu And Kashmir",
    "Jammu And Kashmir (Ihip)":                         "Jammu And Kashmir",
    "Jammu& Kashmir":                                   "Jammu And Kashmir",
    # Jharkhand
    "Jharkhand":                                        "Jharkhand",
    "Jharkhand Jharkhand":                              "Jharkhand",
    # Karnataka
    "Karnataka":                                        "Karnataka",
    "Karnataka Karnataka":                              "Karnataka",
    # Kerala
    "Kerala":                                           "Kerala",
    # Ladakh
    "Ladakh":                                           "Ladakh",
    # Lakshadweep
    "Lakshadweep":                                      "Lakshadweep",
    # Madhya Pradesh
    "Madhya":                                           "Madhya Pradesh",
    "Madhya Pradesh":                                   "Madhya Pradesh",
    "Madhya Pradesh Madhya Pradesh":                    "Madhya Pradesh",
    # Maharashtra
    "Maharashtra":                                      "Maharashtra",
    "Maharashtra (Ihip)":                               "Maharashtra",
    "Maharashtra Maharashtra":                          "Maharashtra",
    # Manipur
    "Manipur":                                          "Manipur",
    # Meghalaya
    "Meghalaya":                                        "Meghalaya",
    # Mizoram
    "Mizoram":                                          "Mizoram",
    # Nagaland
    "Nagaland":                                         "Nagaland",
    # Odisha
    "Odisha":                                           "Odisha",
    "Odisha Odisha":                                    "Odisha",
    "Orissa":                                           "Odisha",
    # Puducherry
    "Pondicherry":                                      "Puducherry",
    "Puducherry":                                       "Puducherry",
    # Punjab
    "Punjab":                                           "Punjab",
    # Rajasthan
    "Rajasthan":                                        "Rajasthan",
    "Rajasthan Rajasthan":                              "Rajasthan",
    # Sikkim
    "Sikkim":                                           "Sikkim",
    # Tamil Nadu
    "Tamil Nadu":                                       "Tamil Nadu",
    "Tamil Nadu Tamil Nadu":                            "Tamil Nadu",
    "Tamilnadu":                                        "Tamil Nadu",
    # Telangana
    "Telangana":                                        "Telangana",
    # Tripura
    "Tripura":                                          "Tripura",
    # Uttar Pradesh
    "Uttar Pradesh":                                    "Uttar Pradesh",
    "Uttar Pradesh Uttar Pradesh":                      "Uttar Pradesh",
    # Uttarakhand
    "Uttarakhan D":                                     "Uttarakhand",
    "Uttarakhand":                                      "Uttarakhand",
    "Uttarkhand":                                       "Uttarakhand",
    # West Bengal
    "West Bengal":                                      "West Bengal",
    "West Bengal West Bengal":                          "West Bengal",
}

# ─── Disease normalization ────────────────────────────────────────────────────
# Canonical names for known diseases — applied via substring/regex matching.
# Order matters: more specific patterns first.
DISEASE_CANONICAL = [
    # Encephalitis / AES / JE — Chandipura variants first (more specific)
    ("chandipura",                                                          "Chandipura Viral Encephalitis"),
    # AES / JE / Encephalitis (generic)
    ("acute encephalitis|aes|aes/je|aes /je|j.?e$|japanese encephalitis|encephal|encephelit", "Acute Encephalitis Syndrome"),
    # Hsv / Herpes encephalitis
    ("herpes.*encephalitis|hsv.*encephalitis|encephalitis.*herpes|encephalitis.*hsv", "Herpes Encephalitis"),
    # PAM
    ("amoebic|pam",                                                         "Primary Amoebic Meningoencephalitis"),
    # Diarrhoea / Diarrhea
    ("acute diarr|diarr",                                                   "Acute Diarrhoeal Disease"),
    # Dengue + Chikungunya combined
    ("dengue.*chikungunya|chikungunya.*dengue",                             "Dengue And Chikungunya"),
    # Dengue
    ("dengue",                                                              "Dengue"),
    # Chikungunya
    ("chikungunya",                                                         "Chikungunya"),
    # Malaria
    ("malaria",                                                             "Malaria"),
    # Food-borne illness (before generic 'food')
    ("food.?borne|food borne",                                              "Food Poisoning"),
    # Food poisoning
    ("food poison|food-poison",                                             "Food Poisoning"),
    # Alcohol poisoning
    ("alcohol poison",                                                      "Alcohol Poisoning"),
    # Mushroom poisoning
    ("mushroom poison",                                                     "Mushroom Poisoning"),
    # Poisoning (other/unspecified)
    ("poison",                                                              "Poisoning"),
    # Cholera
    ("cholera",                                                             "Cholera"),
    # Typhoid / Enteric fever
    ("typhoid|enteric fever",                                               "Typhoid Fever"),
    # Hepatitis A & E combined (before individual variants)
    ("hepatitis.*[ae].*[ae]|hepatitis a.?e|hepatitis e.?a|hepatitis.*a.*e|hepatitis.*e.*a", "Hepatitis A And E"),
    # Hepatitis A
    ("hepatitis a\b|hepatitis.*\ba\b",                                     "Hepatitis A"),
    # Hepatitis B
    ("hepatitis b",                                                         "Hepatitis B"),
    # Hepatitis C
    ("hepatitis c",                                                         "Hepatitis C"),
    # Hepatitis E
    ("hepatitis e\b|hepatitis.*\be\b",                                     "Hepatitis E"),
    # Hepatitis (unspecified / viral) / Jaundice
    ("hepatit|jaundice",                                                    "Viral Hepatitis"),
    # Measles / Rubella
    ("measles|rubell",                                                      "Measles"),
    # Mumps
    ("mumps",                                                               "Mumps"),
    # Chickenpox / Varicella
    ("chickenpox|varicella",                                                "Chickenpox"),
    # Influenza / H1N1
    ("influenza|h1n1|swine flu|seasonal flu",                              "Influenza H1N1"),
    # COVID
    ("covid",                                                               "COVID-19"),
    # Viral conjunctivitis
    ("conjunct",                                                            "Viral Conjunctivitis"),
    # Scrub typhus
    ("scrub typhus",                                                        "Scrub Typhus"),
    # Leptospirosis
    ("leptospiro",                                                          "Leptospirosis"),
    # Kala-azar
    ("kala.?azar|kala azar|visceral leish",                                "Kala-Azar"),
    # Anthrax
    ("anthrax",                                                             "Anthrax"),
    # Plague
    ("plague",                                                              "Plague"),
    # Rabies
    ("rabies",                                                              "Rabies"),
    # Nipah
    ("nipah",                                                               "Nipah"),
    # Zika
    ("zika",                                                                "Zika Virus Disease"),
    # CCHF
    ("crimean|cchf|congo haemorrhagic|congo hemorrhagic",                  "CCHF"),
    # Diphtheria
    ("diphtheria",                                                          "Diphtheria"),
    # Tetanus
    ("tetanus",                                                             "Tetanus"),
    # Whooping cough / Pertussis
    ("whooping|pertussis",                                                  "Pertussis"),
    # Meningitis
    ("meningitis",                                                          "Meningitis"),
    # Upper respiratory
    ("upper respiratory|urti|uri",                                          "Upper Respiratory Tract Infection"),
    # Acute respiratory illness (before generic 'respiratory')
    ("acute respiratory|respiratory illness|respiratory infection",         "Acute Respiratory Illness"),
    # Rotavirus
    ("rotavirus",                                                           "Rotavirus"),
    # Fever with rash
    ("fever.*rash|rash.*fever",                                             "Fever With Rash"),
    # Fever with joint pain
    ("fever.*joint|joint.*fever",                                           "Fever With Joint Pain"),
    # Gastroenteritis (before generic 'acute')
    ("gastro",                                                              "Gastroenteritis"),
    # Generic acute GE abbreviation
    (r"\bge\b",                                                             "Gastroenteritis"),
    # Acute febrile illness / Fever (generic — last among fever variants)
    ("fever",                                                               "Acute Febrile Illness"),
    # AFP
    (r"\bafp\b",                                                            "Acute Flaccid Paralysis"),
    # Viral (unspecified)
    (r"^viral$",                                                            "Viral Illness"),
]

# Pre-compile patterns for speed
DISEASE_PATTERNS = [
    (re.compile(pat, re.IGNORECASE), canonical)
    for pat, canonical in DISEASE_CANONICAL
]

# Strip roman-numeral prefix (e.g. "Xxii. ", "Iii ", "Xlv. ") and trailing "?"
_ROMAN_PREFIX = re.compile(
    r"^(m{0,4}(cm|cd|d?c{0,3})(xc|xl|l?x{0,3})(ix|iv|v?i{0,3}))[\.\s]+",
    re.IGNORECASE,
)
# Also strip leading Arabic serial numbers: "1. ", "12. "
_NUM_PREFIX = re.compile(r"^\d+[\.\)]\s*")
# Strip trailing "?" and whitespace
_TRAIL_JUNK = re.compile(r"[\?\s]+$")


def clean_disease_name(raw: str) -> str:
    """Strip serial-number prefixes and trailing punctuation."""
    s = _ROMAN_PREFIX.sub("", raw.strip())
    s = _NUM_PREFIX.sub("", s)
    s = _TRAIL_JUNK.sub("", s).strip()
    return s


def normalize_disease(name: str) -> str:
    """Map a cleaned disease string to its canonical form."""
    cleaned = clean_disease_name(name)
    for pattern, canonical in DISEASE_PATTERNS:
        if pattern.search(cleaned):
            return canonical
    return cleaned  # return cleaned (not raw) if no match found


def normalize_state(name: str) -> str:
    """Map a state string to its canonical form."""
    return STATE_MAP.get(name, name)


# ─── Validity guards (junk filter) ────────────────────────────────────────────
VALID_STATES_SET = set(STATE_MAP.keys())

def is_valid_state(s: str) -> bool:
    if JUNK_PATTERN.search(s):
        return False
    return s in VALID_STATES_SET

VALID_DISEASE_FRAGS = {
    "dengue", "malaria", "cholera", "diarr", "typhoid", "chikungunya",
    "measles", "hepatitis", "leptospiro", "encephalitis", "influenza",
    "h1n1", "covid", "scrub", "nipah", "rabies", "chickenpox", "varicella",
    "poison", "anthrax", "plague", "meningitis", "fever", "jaundice",
    "gastro", "kala", "aes", "afp", "mumps", "diphtheria", "tetanus",
    "zika", "respiratory", "whooping", "pertussis", "rotavirus", "cchf",
    "crimean", "congo", "viral", "acute", "outbreak", "illness",
}

def is_valid_disease(d: str) -> bool:
    if JUNK_PATTERN.search(d):
        return False
    d_lower = clean_disease_name(d).lower()
    return any(frag in d_lower for frag in VALID_DISEASE_FRAGS)


# ─── Main ─────────────────────────────────────────────────────────────────────
def process_scraped_data():
    print(f"Loading scraped data from {INPUT_FILE}...")
    if not INPUT_FILE.exists():
        print(f"Error: {INPUT_FILE} not found. Run idsp_scraper.py first.")
        return

    df = pd.read_csv(INPUT_FILE)
    print(f"Loaded {len(df):,} raw outbreak records.")

    # ── Step 1: Drop rows missing core fields ──────────────────────────────────
    df = df.dropna(subset=["state", "disease", "year", "week"]).copy()

    # ── Step 2: Title-case for consistency before mapping ─────────────────────
    df["state"]   = df["state"].astype(str).str.strip().str.title()
    df["disease"] = df["disease"].astype(str).str.strip().str.title()

    # ── Step 3: Filter junk rows ───────────────────────────────────────────────
    before = len(df)
    mask = df["state"].apply(is_valid_state) & df["disease"].apply(is_valid_disease)
    df = df[mask].copy()
    print(f"  Removed {before - len(df):,} junk/header rows → {len(df):,} real records.")

    # ── Step 4: Normalize state names ─────────────────────────────────────────
    df["state"] = df["state"].apply(normalize_state)
    print(f"  States after normalization: {df['state'].nunique()} unique")

    # ── Step 5: Normalize disease names ───────────────────────────────────────
    df["disease"] = df["disease"].apply(normalize_disease)
    print(f"  Diseases after normalization: {df['disease'].nunique()} unique")

    # ── Step 6: Numeric columns ────────────────────────────────────────────────
    df["cases"]  = pd.to_numeric(df["cases"],  errors="coerce").fillna(0).astype(int)
    df["deaths"] = pd.to_numeric(df["deaths"], errors="coerce").fillna(0).astype(int)

    # ── Step 7: Year/Week → Calendar Date ─────────────────────────────────────
    print("Mapping weeks to calendar dates...")
    df["date"] = pd.to_datetime(
        df["year"].astype(int).astype(str) + "-" +
        df["week"].astype(int).astype(str) + "-1",
        format="%Y-%W-%w",
        errors="coerce",
    )
    bad = df["date"].isna() | (abs(df["date"].dt.year - df["year"]) > 1)
    if bad.sum():
        print(f"  ⚠  Dropping {bad.sum()} rows with invalid dates.")
    df = df[~bad].copy()

    # ── Step 8: State-level aggregation ───────────────────────────────────────
    print("Aggregating to state-level summaries...")
    agg = (
        df.groupby(["date", "state", "disease"], as_index=False)
        .agg(total_cases=("cases", "sum"), total_deaths=("deaths", "sum"))
    )
    agg = agg.sort_values(["state", "disease", "date"]).reset_index(drop=True)

    # ── Step 9: Export ─────────────────────────────────────────────────────────
    OUTPUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    agg.to_csv(OUTPUT_FILE, index=False)

    print(f"\n✅ Preprocessing Complete!")
    print(f"   Records  : {len(agg):,}")
    print(f"   States   : {agg['state'].nunique()}")
    print(f"   Diseases : {agg['disease'].nunique()}")
    print(f"   Date range: {agg['date'].min().date()} → {agg['date'].max().date()}")
    print(f"\n   Saved → {OUTPUT_FILE}")

    print("\nUnique canonical diseases:")
    for d in sorted(agg["disease"].unique()):
        print(f"  {d}")


if __name__ == "__main__":
    process_scraped_data()