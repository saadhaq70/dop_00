"""
╔══════════════════════════════════════════════════════════════════╗
║   IDSP Weekly Outbreak Report Scraper  |  2013 – 2025           ║
║   Source: https://idsp.mohfw.gov.in                             ║
║                                                                  ║
║   Usage:                                                         ║
║     pip install -r requirements.txt                             ║
║     python idsp_scraper.py                                       ║
║                                                                  ║
║   Output (in ./idsp_output/):                                    ║
║     idsp_outbreaks_2013_2025.csv   – full dataset                ║
║     idsp_yearly_summary.csv        – aggregated by year          ║
║     idsp_download_log.csv          – per-PDF status              ║
║     idsp_pdf_urls_2013_2025.csv    – all 558 PDF links           ║
╚══════════════════════════════════════════════════════════════════╝
"""

import requests
import pdfplumber
import pandas as pd
import re
import io
import time
import os
from collections import Counter

# ─── Configuration ─────────────────────────────────────────────────────────────
OUTPUT_DIR      = "./idsp_output"
DELAY_SECONDS   = 1.2          # polite delay between requests
REQUEST_TIMEOUT = 30
HEADERS = {
    "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0",
    "Accept":          "application/pdf,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer":         "https://idsp.mohfw.gov.in/",
}

os.makedirs(OUTPUT_DIR, exist_ok=True)


# ─── Google Drive: convert view URL → direct download URL ────────────────────
def gdrive_direct_url(view_url: str) -> str:
    """
    Converts a Google Drive share/view link:
      https://drive.google.com/file/d/FILE_ID/view?usp=sharing
    to a direct download link:
      https://drive.google.com/uc?export=download&id=FILE_ID
    """
    import re as _re
    m = _re.search(r"/file/d/([^/]+)/", view_url)
    if m:
        return f"https://drive.google.com/uc?export=download&id={m.group(1)}"
    return view_url  # return original if pattern doesn't match


# ─── Helper: ordinal string  (1 → "1st", 2 → "2nd" …) ────────────────────────
def ordinal(n: int) -> str:
    if 11 <= n % 100 <= 13:
        return f"{n}th"
    return f"{n}{['th','st','nd','rd','th','th','th','th','th','th'][n % 10]}"


# ─── All 558 direct PDF URLs (Google-Drive links excluded) ────────────────────
def build_link_table() -> list[dict]:
    """
    Returns a list of dicts: {year, week, url}
    All URLs were extracted live from:
    https://idsp.mohfw.gov.in/index4.php?lang=1&level=0&linkid=406&lid=3689
    """
    raw: dict[int, dict[int, str]] = {}

    # ── 2025 (weeks 1–13 on Google Drive; 14–52 direct) ──────────────────
    raw[2025] = {
        # Google Drive weeks (converted to direct download at scrape time)
         1: "https://drive.google.com/file/d/1S-Nw1WSp_GngcPFZt354ebS8VPBD-DI9/view?usp=sharing",
         2: "https://drive.google.com/file/d/1NTa_RiSHFz2tgglo1MO2Uzlxz3-sMZRY/view?usp=sharing",
         3: "https://drive.google.com/file/d/1sm_Cbbu10StWvDdyxYQ9pCCapLApoBf2/view?usp=sharing",
         4: "https://drive.google.com/file/d/1PS4CQgeY7LhJzzsdmAsAZmtFVV5nlSqn/view?usp=sharing",
         5: "https://drive.google.com/file/d/19MJ_El2lAqjKWs1pw4mDOo7-toNAWOP9/view?usp=sharing",
         6: "https://drive.google.com/file/d/1rB7lRuFPJehunDAEi4fCK0g6gFBAbj11/view?usp=sharing",
         7: "https://drive.google.com/file/d/1WuXv-B_RFfcga4aW517mzPQvaVt8usZ9/view?usp=sharing",
         8: "https://drive.google.com/file/d/1Dn7BmPtXMj23YbosQJDEmh_9V_unjAfb/view?usp=sharing",
         9: "https://drive.google.com/file/d/1Ouw_woI_VEoLfqpB3ohZZienm6bzDTP4/view?usp=sharing",
        10: "https://drive.google.com/file/d/1SlbSsNJcNn8e98GAkw_ZV8RCie5LAMvk/view?usp=drive_link",
        11: "https://drive.google.com/file/d/14pv1Xhz_gN3Z2i-DoeXyMEoyTh-djy9D/view?usp=drive_link",
        12: "https://drive.google.com/file/d/1EfzGMXjKtgCTqPaEwE5SG9gjoFbxVN0k/view?usp=sharing",
        13: "https://drive.google.com/file/d/17KUukYNBjx8JXYRyFWFe9ENe2JFCnPTi/view?usp=sharing",
        # Direct IDSP links (weeks 14–52)
        14: "https://idsp.mohfw.gov.in/WriteReadData/l892s/19859808031748855005.pdf",
        15: "https://idsp.mohfw.gov.in/WriteReadData/l892s/73169914541749106418.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/16111524191749201952.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/83738128151750661230.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/49519545601751257248.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/75788759701752062509.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/46878165591752552468.pdf",
        21: "https://idsp.mohfw.gov.in/WriteReadData/l892s/63892529841752814846.pdf",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/63053630001754370208.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/97741154541754032704.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/40497724221754048530.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/32429740761754893177.pdf",
        26: "https://idsp.mohfw.gov.in/WriteReadData/l892s/34745966901755506823.pdf",
        27: "https://idsp.mohfw.gov.in/WriteReadData/l892s/5346251801755842541.pdf",
        28: "https://idsp.mohfw.gov.in/WriteReadData/l892s/102422351756882459.pdf",
        29: "https://idsp.mohfw.gov.in/WriteReadData/l892s/59365499701757416180.pdf",
        30: "https://idsp.mohfw.gov.in/WriteReadData/l892s/2471833701757912313.pdf",
        31: "https://idsp.mohfw.gov.in/WriteReadData/l892s/37618885271758008500.pdf",
        32: "https://idsp.mohfw.gov.in/WriteReadData/l892s/24391234031758598649.pdf",
        33: "https://idsp.mohfw.gov.in/WriteReadData/l892s/59012980081759128805.pdf",
        34: "https://idsp.mohfw.gov.in/WriteReadData/l892s/73327934151759730706.pdf",
        35: "https://idsp.mohfw.gov.in/WriteReadData/l892s/46133455371760349558.pdf",
        36: "https://idsp.mohfw.gov.in/WriteReadData/l892s/55664559961761716220.pdf",
        37: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4345056491761883941.pdf",
        38: "https://idsp.mohfw.gov.in/WriteReadData/l892s/805895871763230500.pdf",
        39: "https://idsp.mohfw.gov.in/WriteReadData/l892s/81262998051763230366.pdf",
        40: "https://idsp.mohfw.gov.in/WriteReadData/l892s/31351332071764654053.pdf",
        41: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4474994691764654405.pdf",
        42: "https://idsp.mohfw.gov.in/WriteReadData/l892s/72841139511764924759.pdf",
        43: "https://idsp.mohfw.gov.in/WriteReadData/l892s/41131162841765953603.pdf",
        44: "https://idsp.mohfw.gov.in/WriteReadData/l892s/9182768631766058094.pdf",
        45: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1382217141766997965.pdf",
        46: "https://idsp.mohfw.gov.in/WriteReadData/l892s/85622695501767589998.pdf",
        47: "https://idsp.mohfw.gov.in/WriteReadData/l892s/98575532841768293928.pdf",
        48: "https://idsp.mohfw.gov.in/WriteReadData/l892s/89531768051768972281.pdf",
        49: "https://idsp.mohfw.gov.in/WriteReadData/l892s/86962910081768972316.pdf",
        50: "https://idsp.mohfw.gov.in/WriteReadData/l892s/58225284751768980062.pdf",
        51: "https://idsp.mohfw.gov.in/WriteReadData/l892s/65130675251770705678.pdf",
        52: "https://idsp.mohfw.gov.in/WriteReadData/l892s/24596635601770894009.pdf",
    }

    # ── 2024 (weeks 1-3 and 5-13 on Google Drive; weeks 14-52 direct) ────────
    raw[2024] = {
        # Google Drive weeks (converted to direct download at scrape time)
         1: "https://drive.google.com/file/d/1SsmjJf-wrTtau9DM5YK3qthByCrmK7JE/view?usp=sharing",
         2: "https://drive.google.com/file/d/1LJdSIdb0D3WTdVSw_HkwVKJKaFU1gIA-/view?usp=sharing",
         3: "https://drive.google.com/file/d/1BUQRAAr6jqLY1U6po6FwpfCEAErCCoEu/view?usp=sharing",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/81470887121748948191.pdf",
         5: "https://drive.google.com/file/d/1ugVynaeUhTSP9wSPJEpnyFvm-yDwJYWD/view?usp=drive_link",
         6: "https://drive.google.com/file/d/171CIVyUf_g8WWV5iy-BBsxOJ-w3TjxeK/view?usp=sharing",
         7: "https://drive.google.com/file/d/1p084F_Nkx3PvKFTnIUxUwfeT62epyzkX/view?usp=sharing",
         8: "https://drive.google.com/file/d/1RPKEGDrNDELaz3LKfCCY9NC8kC8PufB4/view?usp=sharing",
         9: "https://drive.google.com/file/d/1oOCt8avv_yfOlsQ047XjCwtEOM5dq5CC/view?usp=sharing",
        10: "https://drive.google.com/file/d/1lumtRYDosARGeEKcHfggt5epdtyitS1A/view?usp=sharing",
        11: "https://drive.google.com/file/d/1qYHV0dODb1_v3VgI49O-x5MAjMYYThBs/view?usp=sharing",
        12: "https://drive.google.com/file/d/1gYke6h08qbm30tgTqi88YvneBSmh3cZW/view?usp=sharing",
        13: "https://drive.google.com/file/d/1FdbshEogmU7Ut4wY8iNKEVDE6J-4gGm_/view?usp=sharing",
        # Direct IDSP links (weeks 14-52)
        14: "https://drive.google.com/file/d/1VYR3JHc7T7bMnk8r7j83OfQHeJ4p_qJt/view?usp=sharing",
        15: "https://drive.google.com/file/d/1p93pnw30hl6Dsg1Tz3UooGaX9ifs7bPS/view?usp=sharing",
        16: "https://drive.google.com/file/d/1Z4wqatbeSAiQogSZpSyfh0TPhtsIGbpO/view?usp=sharing",
        17: "https://drive.google.com/file/d/181r-U1xoqvGFyer9pcRBisV_n23W5G90/view?usp=sharing",
        18: "https://drive.google.com/file/d/1bnbn-nVYBADrMvxH67GkNaPq87YwzTYB/view?usp=sharing",
        19: "https://drive.google.com/file/d/1X0AHVK_AExD2cRKNk-7K5umZB_YdDMSR/view?usp=sharing",
        20: "https://drive.google.com/file/d/1BFi8uM9DDwb0Q-X-1vJ4pCiUJqMWryAe/view?usp=sharing",
        21: "https://drive.google.com/file/d/1JtZCMGp5-wNBbBYv02xdCujs8r-ba6uh/view?usp=sharing",
        22: "https://drive.google.com/file/d/1J3BzC_ocEoWG2nbQiuRSvfbD7h1Vw0Bw/view?usp=sharing",
        23: "https://drive.google.com/file/d/1oW5w73kaCCnaeJmQU6lJOwy-afvX_5O3/view?usp=sharing",
        24: "https://drive.google.com/file/d/1JquWgitb12IqEsQh34sX1IQ5g9L2IiW6/view?usp=sharing",
        25: "https://drive.google.com/file/d/1zNmXoGphKZJBRg5fv2ak9rOFS7isXJqv/view?usp=sharing",
        26: "https://drive.google.com/file/d/1nqmrT1K0yMGVLubn_1c3OR5Tjh7OmIW1/view?usp=sharing",
        27: "https://drive.google.com/file/d/1RuBZeJLDaedV8Vj7Y4RPdvv05rGbqiPD/view?usp=sharing",
        28: "https://drive.google.com/file/d/1n4uySrCezHyJbLipKwwh1KfEewON_gDh/view?usp=sharing",
        29: "https://drive.google.com/file/d/1PtXxrZ4jXTUqN93sMi1mds74n-oxCrOm/view?usp=sharing",
        30: "https://drive.google.com/file/d/1iJ0y1mspBD68tD58TOCT75c-7YBzlo5L/view?usp=sharing",
        31: "https://drive.google.com/file/d/1lKaVabDcJwf64Cj3C_v-IbLkA878JHxI/view?usp=sharing",
        32: "https://drive.google.com/file/d/1R48Z4mop81jCe8hRRq-yONUDo0y3ZRtt/view?usp=sharing",
        33: "https://drive.google.com/file/d/1gZ3m-gJslKuzmCdhnit_Ka1au9-P8DTt/view?usp=sharing",
        34: "https://drive.google.com/file/d/1sc7KamA4UXuB9YImoHPyO9yVAzEnChZ9/view?usp=sharing",
        35: "https://drive.google.com/file/d/1H1pLNH7m8t-Fi_bqPC1O7rjYY1BPgPSz/view?usp=sharing",
        36: "https://drive.google.com/file/d/18fDoWSo9C7hGzSpYWdxI65CfNejBXhfH/view?usp=sharing",
        37: "https://drive.google.com/file/d/1gZY4wd-G9b8Xk_3L18ZO1H17FsDGnTR7/view?usp=sharing",
        38: "https://drive.google.com/file/d/1E8QA8_G2Z6GnlDWwvE2VCa9JAf2kykDK/view?usp=sharing",
        39: "https://drive.google.com/file/d/1U9yLalb31zQCj8Sx6XVNqvSSl-j4rm_J/view?usp=sharing",
        40: "https://drive.google.com/file/d/1i-fun3UzME2SCF9TbM6EKJ3A12EBW0_L/view?usp=sharing",
        41: "https://drive.google.com/file/d/1Cpr_45rvsaUqWiA3pmumANhkOcHo5o1i/view?usp=sharing",
        42: "https://drive.google.com/file/d/1fdXlPXw4utZVWk5bevJ3B0lFKH7dmKNr/view?usp=sharing",
        43: "https://drive.google.com/file/d/1ckrHLT9YO4ioQ7B0PQX1jlsSGkm_AsQT/view?usp=drive_link",
        44: "https://drive.google.com/file/d/1IODkFK_BavqO6G38T5jFvlzkOpUjn3En/view?usp=drive_link",
        45: "https://drive.google.com/file/d/1wsWUF7CyysoU1kObUGMj8AEkd_NdDfWb/view?usp=sharing",
        46: "https://drive.google.com/file/d/1aEF2AI-e-XLx29QWfA4JBAWsSmnu9QcZ/view?usp=sharing",
        47: "https://drive.google.com/file/d/1gpGTcGuBf4ZyDJK6FXMsu9yxzw0WsiYi/view?usp=sharing",
        48: "https://drive.google.com/file/d/1Wj4RIO8qr0ofT__7MdINowHUKXDe9x6l/view?usp=sharing",
        49: "https://drive.google.com/file/d/1Ll2rwBUaPWq9q9BKPCRQGYnQ9Jwgk2dp/view?usp=sharing",
        50: "https://drive.google.com/file/d/1WL87VTQwhGyTmpCGGi3XanbcO4Ltj5cb/view?usp=sharing",
        51: "https://drive.google.com/file/d/1nLk5mRjguqOvbz__-6rFWBg7H0HvmRau/view?usp=sharing",
        52: "https://drive.google.com/file/d/1KSBt394Clc-njJck9l-MsxsO8wiZEP0_/view?usp=sharing",
    }

    # ── 2023 (mix of direct + Google Drive links) ───────────────────────
    raw[2023] = {
         1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/56953886141677569586.pdf",
         2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/5598982481677737839.pdf",
         3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/59609550111678770735.pdf",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/22483332931678770810.pdf",
         5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4298719451679310429.pdf",
         6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/43588480701679571484.pdf",
         7: "https://idsp.mohfw.gov.in/WriteReadData/l892s/92837229251680676582.pdf",
         8: "https://idsp.mohfw.gov.in/WriteReadData/l892s/959182831681282940.pdf",
         9: "https://idsp.mohfw.gov.in/WriteReadData/l892s/5334491831681890297.pdf",
        10: "https://idsp.mohfw.gov.in/WriteReadData/l892s/24421597211682506215.pdf",
        11: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90932843861683201733.pdf",
        12: "https://idsp.mohfw.gov.in/WriteReadData/l892s/87996730991684214595.pdf",
        13: "https://idsp.mohfw.gov.in/WriteReadData/l892s/19237813091684214647.pdf",
        14: "https://idsp.mohfw.gov.in/WriteReadData/l892s/58269660581685423781.pdf",
        15: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3439901971685510753.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4625928981686305861.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/48854385361688450498.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/8984621471688550207.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/55850372501688721705.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/59542223571689673947.pdf",
        21: "https://idsp.mohfw.gov.in/WriteReadData/l892s/64657729271690964415.pdf",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/9050581631691147080.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/15879821711692006852.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/23869537881693295660.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3934934481693388430.pdf",
        # Google Drive weeks
        26: "https://drive.google.com/file/d/10tr8uV-XSxl0YymY9kEYFlVN5Q5FuUnV/view?usp=sharing",
        27: "https://drive.google.com/file/d/11IOaiQ4kyPfmHKyKbD__BvJw0rXYgxqT/view?usp=sharing",
        28: "https://drive.google.com/file/d/13aRKLFiItK2cf_UvPEJPNjqi1BfVhOgQ/view?usp=sharing",
        29: "https://drive.google.com/file/d/1TJegZJHdb9mY3Lmep4ZmBWvDm-gEKlZn/view?usp=sharing",
        30: "https://drive.google.com/file/d/1CeI73epXkepY7G4zhRgDLey97RCg9EWM/view?usp=sharing",
        31: "https://drive.google.com/file/d/1WbqirOXgbmGUZ4RJm2lstPviLlfRJDDI/view?usp=sharing",
        32: "https://drive.google.com/file/d/16IxSuOvE-dNVNzSQi-RVFRO21tAFxBNU/view?usp=sharing",
        33: "https://idsp.mohfw.gov.in/WriteReadData/l892s/33666127771697525084.pdf",
        34: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1036560991697701673.pdf",
        35: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6334518031698667764.pdf",
        36: "https://idsp.mohfw.gov.in/WriteReadData/l892s/128660161699611747.pdf",
        37: "https://idsp.mohfw.gov.in/WriteReadData/l892s/47975176701700192645.pdf",
        38: "https://idsp.mohfw.gov.in/WriteReadData/l892s/57241746311700469239.pdf",
        39: "https://drive.google.com/file/d/1zYcQ-s1bn2oIZC7s75StEv3gfH7-SG9m/view?usp=sharing",
        40: "https://drive.google.com/file/d/1_Eld-OoL2R7Jep_egDvTejeVoA1UqZHH/view?usp=sharing",
        41: "https://drive.google.com/file/d/1bfBuLDqCgReHfZmfn0vGSishe-joRspF/view?usp=sharing",
        42: "https://idsp.mohfw.gov.in/WriteReadData/l892s/17113853891702917780.pdf",
        43: "https://drive.google.com/file/d/1S2NwWg4kkwG5JzyucpfaekuTb7M7Y338/view?usp=sharing",
        44: "https://idsp.mohfw.gov.in/WriteReadData/l892s/11684956991704779982.pdf",
        45: "https://idsp.mohfw.gov.in/WriteReadData/l892s/58496833651705032329.pdf",
        46: "https://idsp.mohfw.gov.in/WriteReadData/l892s/17889938321705308095.pdf",
        47: "https://drive.google.com/file/d/1ACnjp1gpgdeAlhbzjvolAjb8O8_Rtizb/view?usp=sharing",
        48: "https://drive.google.com/file/d/12j_m_utGsNBRwiEckjuwzSCdpcFvrLlO/view?usp=sharing",
        49: "https://drive.google.com/file/d/1lABEzjOsFk3hNk-BBFyc7vVRO773XpHN/view?usp=sharing",
        50: "https://drive.google.com/file/d/1EOIJyqtZLVjDIz1q7-sjZBn6hMFM1bZ9/view?usp=sharing",
        51: "https://drive.google.com/file/d/1N1nx9L9PvucIeHPh0s_Y93brf_LFM322/view?usp=sharing",
        52: "https://drive.google.com/file/d/1ruCBZP_Cgn85cFd7KPeihNaLg8264gNz/view?usp=sharing",
    }

    # ── 2022 (all 52 weeks direct) ────────────────────────────────────────
    raw[2022] = {
         1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/2548782891644495164.pdf",
         2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7295787021644495213.pdf",
         3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/5146664691645083574.pdf",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/89343299521646644982.pdf",
         5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/70457157021646826766.pdf",
         6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/50723308761648360611.pdf",
         7: "https://idsp.mohfw.gov.in/WriteReadData/l892s/80255800071648722166.pdf",
         8: "https://idsp.mohfw.gov.in/WriteReadData/l892s/62747270011649328179.pdf",
         9: "https://idsp.mohfw.gov.in/WriteReadData/l892s/35671841001650605515.pdf",
        10: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1392278531651211316.pdf",
        11: "https://idsp.mohfw.gov.in/WriteReadData/l892s/17777132131651643473.pdf",
        12: "https://idsp.mohfw.gov.in/WriteReadData/l892s/47642712261652251458.pdf",
        13: "https://idsp.mohfw.gov.in/WriteReadData/l892s/27714369291652936295.pdf",
        14: "https://idsp.mohfw.gov.in/WriteReadData/l892s/25631695521654237028.pdf",
        15: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3440912171654257519.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/8769878311655962403.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7494517571658218152.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1679188201658218211.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/25831535421658994103.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/17460277921659429678.pdf",
        21: "https://idsp.mohfw.gov.in/WriteReadData/l892s/48206806551659700618.pdf",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/69983681661495600.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1409615621661929367.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/33555288411662452396.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/10534460671663568873.pdf",
        26: "https://idsp.mohfw.gov.in/WriteReadData/l892s/97322230781664263759.pdf",
        27: "https://idsp.mohfw.gov.in/WriteReadData/l892s/74187654281664790779.pdf",
        28: "https://idsp.mohfw.gov.in/WriteReadData/l892s/83459743991665057580.pdf",
        29: "https://idsp.mohfw.gov.in/WriteReadData/l892s/80752570771665481474.pdf",
        30: "https://idsp.mohfw.gov.in/WriteReadData/l892s/79431194251666685561.pdf",
        31: "https://idsp.mohfw.gov.in/WriteReadData/l892s/50518998681666778173.pdf",
        32: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4064579051667198471.pdf",
        33: "https://idsp.mohfw.gov.in/WriteReadData/l892s/45756211601667459801.pdf",
        34: "https://idsp.mohfw.gov.in/WriteReadData/l892s/73482782211667971918.pdf",
        35: "https://idsp.mohfw.gov.in/WriteReadData/l892s/94446467611669198494.pdf",
        36: "https://idsp.mohfw.gov.in/WriteReadData/l892s/67492866371669284742.pdf",
        37: "https://idsp.mohfw.gov.in/WriteReadData/l892s/75891972881670501228.pdf",
        38: "https://idsp.mohfw.gov.in/WriteReadData/l892s/78391596911671087086.pdf",
        39: "https://idsp.mohfw.gov.in/WriteReadData/l892s/11984820951671616638.pdf",
        40: "https://idsp.mohfw.gov.in/WriteReadData/l892s/42160159931671776389.pdf",
        41: "https://idsp.mohfw.gov.in/WriteReadData/l892s/51700607561672038266.pdf",
        42: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1430537991672296570.pdf",
        43: "https://idsp.mohfw.gov.in/WriteReadData/l892s/25411693011672911468.pdf",
        44: "https://idsp.mohfw.gov.in/WriteReadData/l892s/8511094351673004835.pdf",
        45: "https://idsp.mohfw.gov.in/WriteReadData/l892s/31213653551673410239.pdf",
        46: "https://idsp.mohfw.gov.in/WriteReadData/l892s/60008466191674111579.pdf",
        47: "https://idsp.mohfw.gov.in/WriteReadData/l892s/39954396811675250534.pdf",
        48: "https://idsp.mohfw.gov.in/WriteReadData/l892s/66557506361675679880.pdf",
        49: "https://idsp.mohfw.gov.in/WriteReadData/l892s/16602315821676010289.pdf",
        50: "https://idsp.mohfw.gov.in/WriteReadData/l892s/2882454301676010303.pdf",
        51: "https://idsp.mohfw.gov.in/WriteReadData/l892s/37421173271676361045.pdf",
        52: "https://idsp.mohfw.gov.in/WriteReadData/l892s/54855544991676459270.pdf",
    }

    # ── 2021 ──────────────────────────────────────────────────────────────
    raw[2021] = {
         1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/54588936701614162099.pdf",
         2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/65498445311614162134.pdf",
         3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/74204773541614676896.pdf",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4803207561615807414.pdf",
         5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4197231291617278271.pdf",
         6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/63739555761617691316.pdf",
         7: "https://idsp.mohfw.gov.in/WriteReadData/l892s/40381941051618291545.pdf",
         8: "https://idsp.mohfw.gov.in/WriteReadData/l892s/92677823771621846952.pdf",
         9: "https://idsp.mohfw.gov.in/WriteReadData/l892s/70486229221621846974.pdf",
        10: "https://idsp.mohfw.gov.in/WriteReadData/l892s/30875961491622115622.pdf",
        11: "https://idsp.mohfw.gov.in/WriteReadData/l892s/75398645741622451261.pdf",
        12: "https://idsp.mohfw.gov.in/WriteReadData/l892s/59880574731623130107.pdf",
        13: "https://idsp.mohfw.gov.in/WriteReadData/l892s/97509421901623653932.pdf",
        14: "https://idsp.mohfw.gov.in/WriteReadData/l892s/94994875221624427606.pdf",
        15: "https://idsp.mohfw.gov.in/WriteReadData/l892s/50142595531625035099.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/50964838001625218861.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/52349419351625728884.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/73666412961626684927.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/89997464371627471359.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/77905767661628494849.pdf",
        21: "https://idsp.mohfw.gov.in/WriteReadData/l892s/54136961551628675774.pdf",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/10751456901629710010.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/71666443691631189657.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3818378521631189682.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/25286157331631579632.pdf",
        26: "https://idsp.mohfw.gov.in/WriteReadData/l892s/12830469691632485260.pdf",
        27: "https://idsp.mohfw.gov.in/WriteReadData/l892s/93858438291632994388.pdf",
        28: "https://idsp.mohfw.gov.in/WriteReadData/l892s/47953744461634031895.pdf",
        29: "https://idsp.mohfw.gov.in/WriteReadData/l892s/42514392891634203744.pdf",
        30: "https://idsp.mohfw.gov.in/WriteReadData/l892s/53945400761634203762.pdf",
        31: "https://idsp.mohfw.gov.in/WriteReadData/l892s/74168424891635145233.pdf",
        32: "https://idsp.mohfw.gov.in/WriteReadData/l892s/60690508801635853024.pdf",
        33: "https://idsp.mohfw.gov.in/WriteReadData/l892s/71204470671637580725.pdf",
        34: "https://idsp.mohfw.gov.in/WriteReadData/l892s/79268315211637580936.pdf",
        35: "https://idsp.mohfw.gov.in/WriteReadData/l892s/67323893291638446578.pdf",
        36: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7803926841638446619.pdf",
        37: "https://idsp.mohfw.gov.in/WriteReadData/l892s/68142848861638769472.pdf",
        38: "https://idsp.mohfw.gov.in/WriteReadData/l892s/12628276741640844117.pdf",
        39: "https://idsp.mohfw.gov.in/WriteReadData/l892s/78048457291640928892.pdf",
        40: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90987822861641383977.pdf",
        41: "https://idsp.mohfw.gov.in/WriteReadData/l892s/11273184721641384072.pdf",
        42: "https://idsp.mohfw.gov.in/WriteReadData/l892s/49912946621641555899.pdf",
        43: "https://idsp.mohfw.gov.in/WriteReadData/l892s/12385261621641816039.pdf",
        44: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4597934061641988247.pdf",
        45: "https://idsp.mohfw.gov.in/WriteReadData/l892s/37476946981642145276.pdf",
        46: "https://idsp.mohfw.gov.in/WriteReadData/l892s/37155971601642407309.pdf",
        47: "https://idsp.mohfw.gov.in/WriteReadData/l892s/99070210871642594974.pdf",
        48: "https://idsp.mohfw.gov.in/WriteReadData/l892s/907026181642679041.pdf",
        49: "https://idsp.mohfw.gov.in/WriteReadData/l892s/63934703921642751757.pdf",
        50: "https://idsp.mohfw.gov.in/WriteReadData/l892s/67474475281643264551.pdf",
        51: "https://idsp.mohfw.gov.in/WriteReadData/l892s/42140533011643607466.pdf",
        52: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3693633111643607555.pdf",
    }

    # ── 2020 ──────────────────────────────────────────────────────────────
    raw[2020] = {
         1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/48719514701583384078.pdf",
         2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/27385427641583741592.pdf",
         3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/39159205151584345522.pdf",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/76464278111586340109.pdf",
         5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/91997163121586426717.pdf",
         6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/8960781311589282489.pdf",
         7: "https://idsp.mohfw.gov.in/WriteReadData/l892s/55963304211589526100.pdf",
         8: "https://idsp.mohfw.gov.in/WriteReadData/l892s/55485940871590493139.pdf",
         9: "https://idsp.mohfw.gov.in/WriteReadData/l892s/58051976421592202723.pdf",
        10: "https://idsp.mohfw.gov.in/WriteReadData/l892s/34059301592392433.pdf",
        11: "https://idsp.mohfw.gov.in/WriteReadData/l892s/63008264611592915163.pdf",
        12: "https://idsp.mohfw.gov.in/WriteReadData/l892s/72984700061593157501.pdf",
        13: "https://idsp.mohfw.gov.in/WriteReadData/l892s/47749922001595312153.pdf",
        14: "https://idsp.mohfw.gov.in/WriteReadData/l892s/75910552131595312482.pdf",
        15: "https://idsp.mohfw.gov.in/WriteReadData/l892s/82748353081596538437.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/34230801596538550.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/49994483731596602292.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/5964152161596712701.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/26128241981597124552.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/57704139231597648582.pdf",
        21: "https://drive.google.com/file/d/1IpcZap2hw84ELWDYYkCYpKj-7F1gHHiD/view?usp=sharing",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/12542165061598342727.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/26841813001598859859.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/40169163971598859879.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/72315328771599117641.pdf",
        26: "https://idsp.mohfw.gov.in/WriteReadData/l892s/82652121441599205047.pdf",
        27: "https://idsp.mohfw.gov.in/WriteReadData/l892s/76245669481599205071.pdf",
        28: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7996614701600316906.pdf",
        29: "https://idsp.mohfw.gov.in/WriteReadData/l892s/75493226621600316927.pdf",
        30: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6229883631601365884.pdf",
        31: "https://idsp.mohfw.gov.in/WriteReadData/l892s/81792301181601365912.pdf",
        32: "https://idsp.mohfw.gov.in/WriteReadData/l892s/76345468981602073147.pdf",
        33: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3900624281602495904.pdf",
        34: "https://idsp.mohfw.gov.in/WriteReadData/l892s/79615858811603084941.pdf",
        35: "https://idsp.mohfw.gov.in/WriteReadData/l892s/20155875121604654822.pdf",
        36: "https://idsp.mohfw.gov.in/WriteReadData/l892s/94317400911604655031.pdf",
        37: "https://idsp.mohfw.gov.in/WriteReadData/l892s/42935830501604991853.pdf",
        38: "https://idsp.mohfw.gov.in/WriteReadData/l892s/35676360391606195472.pdf",
        39: "https://idsp.mohfw.gov.in/WriteReadData/l892s/64995502961606195427.pdf",
        40: "https://idsp.mohfw.gov.in/WriteReadData/l892s/35786420331606195227.pdf",
        41: "https://idsp.mohfw.gov.in/WriteReadData/l892s/99595557981606195263.pdf",
        42: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6061046811606368261.pdf",
        43: "https://idsp.mohfw.gov.in/WriteReadData/l892s/57351711651607418821.pdf",
        44: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90708520581607945512.pdf",
        45: "https://idsp.mohfw.gov.in/WriteReadData/l892s/57192284951607940109.pdf",
        47: "https://idsp.mohfw.gov.in/WriteReadData/l892s/88490139401610000902.pdf",
        48: "https://idsp.mohfw.gov.in/WriteReadData/l892s/14887303501610440276.pdf",
        49: "https://idsp.mohfw.gov.in/WriteReadData/l892s/8181197221610615473.pdf",
        50: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90829141301611039090.pdf",
        51: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7600145861611826717.pdf",
        52: "https://idsp.mohfw.gov.in/WriteReadData/l892s/21592462931612246615.pdf",
        53: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6601227421612941709.pdf",
    }

    # ── 2019 ──────────────────────────────────────────────────────────────
    raw[2019] = {
         1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/012019.pdf",
         2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/022019.pdf",
         3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/032019.pdf",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/042019.pdf",
         5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/052019.pdf",
         6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/062019.pdf",
         7: "https://idsp.mohfw.gov.in/WriteReadData/l892s/70114365031553578502.pdf",
         8: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3490631051553578944.pdf",
         9: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6589945201554276858.pdf",
        10: "https://idsp.mohfw.gov.in/WriteReadData/l892s/99151176221556005805.pdf",
        11: "https://idsp.mohfw.gov.in/WriteReadData/l892s/58135622001555071060.pdf",
        12: "https://idsp.mohfw.gov.in/WriteReadData/l892s/11468971641556005847.pdf",
        13: "https://idsp.mohfw.gov.in/WriteReadData/l892s/17603578201556280184.pdf",
        14: "https://idsp.mohfw.gov.in/WriteReadData/l892s/67310881641556708881.pdf",
        15: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6590096481557832331.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/21323410851557989924.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/88599375341559045751.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7712669551559045817.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/80647405011559539826.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/55569857191561115369.pdf",
        21: "https://idsp.mohfw.gov.in/WriteReadData/l892s/42153183271561703061.pdf",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/82640712431561703101.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/85360816881563359501.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/7691730161563359625.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/25756315101563359659.pdf",
        26: "https://idsp.mohfw.gov.in/WriteReadData/l892s/68942651301564989114.pdf",
        27: "https://idsp.mohfw.gov.in/WriteReadData/l892s/35559832711564989155.pdf",
        28: "https://idsp.mohfw.gov.in/WriteReadData/l892s/45766197691565696325.pdf",
        29: "https://idsp.mohfw.gov.in/WriteReadData/l892s/45717479631566903502.pdf",
        30: "https://idsp.mohfw.gov.in/WriteReadData/l892s/61242793991568706218.pdf",
        31: "https://idsp.mohfw.gov.in/WriteReadData/l892s/28346714001569322122.pdf",
        32: "https://idsp.mohfw.gov.in/WriteReadData/l892s/3386018991569841125.pdf",
        33: "https://idsp.mohfw.gov.in/WriteReadData/l892s/74743699461571052888.pdf",
        34: "https://idsp.mohfw.gov.in/WriteReadData/l892s/29559503671572417006.pdf",
        35: "https://idsp.mohfw.gov.in/WriteReadData/l892s/77063109021572417436.pdf",
        36: "https://idsp.mohfw.gov.in/WriteReadData/l892s/82656745161572606554.pdf",
        37: "https://idsp.mohfw.gov.in/WriteReadData/l892s/70521842991573621699.pdf",
        38: "https://idsp.mohfw.gov.in/WriteReadData/l892s/84412656471574839783.pdf",
        39: "https://idsp.mohfw.gov.in/WriteReadData/l892s/75110479251574835379.pdf",
        40: "https://idsp.mohfw.gov.in/WriteReadData/l892s/17724297961576151118.pdf",
        41: "https://idsp.mohfw.gov.in/WriteReadData/l892s/81692474481576480348.pdf",
        42: "https://idsp.mohfw.gov.in/WriteReadData/l892s/95714695921576827903.pdf",
        43: "https://idsp.mohfw.gov.in/WriteReadData/l892s/5840814251577708329.pdf",
        44: "https://idsp.mohfw.gov.in/WriteReadData/l892s/4836479471577875954.pdf",
        45: "https://idsp.mohfw.gov.in/WriteReadData/l892s/57227886241578632023.pdf",
        46: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90035950711578899621.pdf",
        47: "https://idsp.mohfw.gov.in/WriteReadData/l892s/2211220741579085810.pdf",
        48: "https://idsp.mohfw.gov.in/WriteReadData/l892s/73655817411579588506.pdf",
        49: "https://idsp.mohfw.gov.in/WriteReadData/l892s/87432761971580215568.pdf",
        50: "https://idsp.mohfw.gov.in/WriteReadData/l892s/51324836071580451844.pdf",
        51: "https://idsp.mohfw.gov.in/WriteReadData/l892s/16441864901581914600.pdf",
        52: "https://idsp.mohfw.gov.in/WriteReadData/l892s/59318143211581914621.pdf",
    }

    # ── 2018 (standard NNyyyy.pdf pattern) ────────────────────────────────
    raw[2018] = {w: f"https://idsp.mohfw.gov.in/WriteReadData/l892s/{w:02d}2018.pdf"
                 for w in range(1, 53)}

    # ── 2017 (weeks 1–6 have different URLs; 7–52 use standard pattern) ───
    raw[2017] = {
        1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/16761928221487223715.pdf",
        2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1489553831487223752.pdf",
        3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6867458831488435370.pdf",
        4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/15366628751488435411.pdf",
        5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/60441234271488435446.pdf",
        6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/77987201021489753657.pdf",
    }
    for w in range(7, 53):
        raw[2017][w] = f"https://idsp.mohfw.gov.in/WriteReadData/l892s/{w:02d}2017.pdf"

    # ── 2016 (weeks 32–52 use standard; earlier weeks have unique URLs) ───
    raw[2016] = {}
    raw[2016].update({
         1: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90928316461465987313.pdf",
         2: "https://idsp.mohfw.gov.in/WriteReadData/l892s/6606507871465987350.pdf",
         3: "https://idsp.mohfw.gov.in/WriteReadData/l892s/2609129231465987381.pdf",
         4: "https://idsp.mohfw.gov.in/WriteReadData/l892s/27847935961465987415.pdf",
         5: "https://idsp.mohfw.gov.in/WriteReadData/l892s/44784235121465987444.pdf",
         6: "https://idsp.mohfw.gov.in/WriteReadData/l892s/70863384721465987475.pdf",
         7: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1544826741465987503.pdf",
         8: "https://idsp.mohfw.gov.in/WriteReadData/l892s/68623290411465987526.pdf",
         9: "https://idsp.mohfw.gov.in/WriteReadData/DOB2016/9th_wk16.pdf",
        10: "https://idsp.mohfw.gov.in/WriteReadData/DOB2016/10th_wk16.pdf",
        11: "https://idsp.mohfw.gov.in/WriteReadData/DOB2016/11th_wk16.pdf",
        12: "https://idsp.mohfw.gov.in/WriteReadData/DOB2016/12th_wk16.pdf",
        13: "https://idsp.mohfw.gov.in/WriteReadData/DOB2016/13th_wk16.pdf",
        14: "https://idsp.mohfw.gov.in/WriteReadData/DOB2016/rcntobrk.pdf",
        16: "https://idsp.mohfw.gov.in/WriteReadData/l892s/99240760141465983634.pdf",
        17: "https://idsp.mohfw.gov.in/WriteReadData/l892s/99171777471465983686.pdf",
        18: "https://idsp.mohfw.gov.in/WriteReadData/l892s/72914162941466750136.pdf",
        19: "https://idsp.mohfw.gov.in/WriteReadData/l892s/95133348011466750283.pdf",
        20: "https://idsp.mohfw.gov.in/WriteReadData/l892s/2958396131466750320.pdf",
        21: "https://idsp.mohfw.gov.in/WriteReadData/l892s/24529831491467368085.pdf",
        22: "https://idsp.mohfw.gov.in/WriteReadData/l892s/73651938401470919034.pdf",
        23: "https://idsp.mohfw.gov.in/WriteReadData/l892s/1017710531470919065.pdf",
        24: "https://idsp.mohfw.gov.in/WriteReadData/l892s/66156980721470919091.pdf",
        25: "https://idsp.mohfw.gov.in/WriteReadData/l892s/72219634171470919115.pdf",
        26: "https://idsp.mohfw.gov.in/WriteReadData/l892s/33166532671470919134.pdf",
        27: "https://idsp.mohfw.gov.in/WriteReadData/l892s/69286944421470997079.pdf",
        28: "https://idsp.mohfw.gov.in/WriteReadData/l892s/81165166051471513557.pdf",
        29: "https://idsp.mohfw.gov.in/WriteReadData/l892s/90160646731472206250.pdf",
        30: "https://idsp.mohfw.gov.in/WriteReadData/l892s/31559774371473068473.pdf",
        31: "https://idsp.mohfw.gov.in/WriteReadData/l892s/58448966271473417376.pdf",
    })
    for w in range(32, 53):
        raw[2016][w] = f"https://idsp.mohfw.gov.in/WriteReadData/l892s/{w:02d}2016.pdf"

    # ── 2015, 2014, 2013  (DOBxxxx/Nth_wkYY.pdf pattern) ─────────────────
    for yr, yy, max_w in [(2015, "15", 53), (2014, "14", 52), (2013, "13", 52)]:
        raw[yr] = {}
        for w in range(1, max_w + 1):
            raw[yr][w] = (
                f"https://idsp.mohfw.gov.in/WriteReadData/DOB{yr}/"
                f"{ordinal(w)}_wk{yy}.pdf"
            )

    links = []
    for year in sorted(raw):
        for week in sorted(raw[year]):
            links.append({"year": year, "week": week, "url": raw[year][week]})
    return links


# ─── PDF Extraction ────────────────────────────────────────────────────────────
STATE_NAMES = [
    "Andhra Pradesh","Arunachal Pradesh","Assam","Bihar","Chhattisgarh","Goa",
    "Gujarat","Haryana","Himachal Pradesh","Jharkhand","Karnataka","Kerala",
    "Madhya Pradesh","Maharashtra","Manipur","Meghalaya","Mizoram","Nagaland",
    "Odisha","Punjab","Rajasthan","Sikkim","Tamil Nadu","Telangana","Tripura",
    "Uttar Pradesh","Uttarakhand","West Bengal","Andaman","Chandigarh","Delhi",
    "Jammu","Ladakh","Lakshadweep","Puducherry","J&K",
]

DISEASE_KEYWORDS = [
    "Dengue","Malaria","Cholera","Diarrhea","Diarrhoea","Typhoid","Chikungunya",
    "Measles","Hepatitis","Leptospirosis","Encephalitis","Influenza","H1N1",
    "COVID","Scrub Typhus","Nipah","Rabies","Chickenpox","Varicella",
    "Food Poisoning","Anthrax","Plague","Meningitis","Fever","Jaundice",
    "Gastroenteritis","Kala-azar","AES","AFP","Mumps","Diphtheria","Tetanus",
    "Zika","Acute Respiratory","Whooping Cough","Pertussis","Rotavirus",
]


def extract_table(pdf_bytes: bytes, year: int, week: int) -> list[dict]:
    rows = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            for page in pdf.pages:
                for table in (page.extract_tables() or []):
                    if not table or len(table) < 2:
                        continue
                    # Detect header row
                    hdr_idx = None
                    for i, row in enumerate(table[:6]):
                        txt = " ".join(str(c or "") for c in row).lower()
                        if ("state" in txt or "district" in txt) and \
                           ("disease" in txt or "case" in txt):
                            hdr_idx = i
                            break
                    if hdr_idx is None:
                        continue

                    hdrs = [str(c or "").strip().lower().replace("\n", " ")
                            for c in table[hdr_idx]]

                    def fc(*terms):
                        for t in terms:
                            for i, h in enumerate(hdrs):
                                if t in h:
                                    return i
                        return None

                    c_sl   = fc("sl", "s.no", "sno")
                    c_st   = fc("state")
                    c_dist = fc("district")
                    c_dis  = fc("disease", "illness")
                    c_cas  = fc("case", "ill", "affected")
                    c_dth  = fc("death", "fatal")
                    c_wk   = fc("week")
                    c_dt   = fc("date")
                    c_rem  = fc("remark", "comment", "status")

                    if c_st is None and c_dis is None:
                        continue

                    for dr in table[hdr_idx + 1:]:
                        if not dr or all(c is None or str(c).strip() == "" for c in dr):
                            continue

                        def cell(col):
                            if col is not None and col < len(dr):
                                return str(dr[col] or "").strip().replace("\n", " ")
                            return ""

                        st, dis = cell(c_st), cell(c_dis)
                        if not st and not dis:
                            continue
                        if "state" in (st+dis).lower() and "disease" in (st+dis).lower():
                            continue

                        rows.append({
                            "year": year, "week": week,
                            "sl_no":          cell(c_sl),
                            "state":          st,
                            "district":       cell(c_dist),
                            "disease":        dis,
                            "cases":          cell(c_cas),
                            "deaths":         cell(c_dth),
                            "report_week":    cell(c_wk),
                            "date_reporting": cell(c_dt),
                            "remarks":        cell(c_rem),
                        })
    except Exception as e:
        print(f"  ⚠  Table parse error {year}w{week}: {e}")
    return rows


def extract_text_fallback(pdf_bytes: bytes, year: int, week: int) -> list[dict]:
    rows = []
    try:
        with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
            text = "\n".join(p.extract_text() or "" for p in pdf.pages)
        for line in text.split("\n"):
            s = next((x for x in STATE_NAMES if x.lower() in line.lower()), None)
            d = next((x for x in DISEASE_KEYWORDS if x.lower() in line.lower()), None)
            if s and d:
                nums = re.findall(r"\b(\d+)\b", line)
                rows.append({
                    "year": year, "week": week, "sl_no": "",
                    "state": s, "district": "",
                    "disease": d,
                    "cases":  nums[0] if nums else "",
                    "deaths": nums[1] if len(nums) > 1 else "",
                    "report_week": "", "date_reporting": "",
                    "remarks": line.strip()[:250],
                })
    except Exception:
        pass
    return rows


# ─── Main ──────────────────────────────────────────────────────────────────────
def main():
    banner = "IDSP Weekly Outbreak Scraper  |  2013 – 2025"
    print("=" * len(banner))
    print(banner)
    print("=" * len(banner))

    links = build_link_table()
    yc = Counter(l["year"] for l in links)
    print(f"\nTotal PDFs queued: {len(links)}")
    for y in sorted(yc):
        print(f"  {y}: {yc[y]} weekly reports")

    # Save URL reference CSV
    url_csv = f"{OUTPUT_DIR}/idsp_pdf_urls_2013_2025.csv"
    pd.DataFrame(links).to_csv(url_csv, index=False)
    print(f"\nURL list saved → {url_csv}")

    all_records, log_rows = [], []
    ok_count = fail_count = 0

    print("\nDownloading and parsing …\n")
    for link in links:
        year, week, url = link["year"], link["week"], link["url"]
        time.sleep(DELAY_SECONDS)

        try:
            # Convert Google Drive view links to direct download links
            download_url = gdrive_direct_url(url) if "drive.google.com" in url else url
            r = requests.get(download_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            if r.status_code != 200:
                raise ValueError(f"HTTP {r.status_code}")
            if b"%PDF" not in r.content[:10]:
                raise ValueError("Response is not a PDF (may need manual auth for GDrive)")
            pdf_bytes = r.content
        except Exception as e:
            print(f"  ✗ {year} w{week:02d} — {e}")
            log_rows.append({**link, "status": "failed", "records": 0, "error": str(e)})
            fail_count += 1
            continue

        records = extract_table(pdf_bytes, year, week)
        if not records:
            records = extract_text_fallback(pdf_bytes, year, week)

        all_records.extend(records)
        log_rows.append({**link, "status": "ok", "records": len(records), "error": ""})
        ok_count += 1
        print(f"  ✓ {year} w{week:02d}  — {len(records):3d} outbreak records")

    # ── Build and save main dataset ─────────────────────────────────────────
    print(f"\n{'─'*50}")
    print(f"Downloaded: {ok_count}  |  Failed: {fail_count}")

    df = pd.DataFrame(all_records)
    if not df.empty:
        df["cases"]  = pd.to_numeric(df["cases"],  errors="coerce")
        df["deaths"] = pd.to_numeric(df["deaths"], errors="coerce")
        for col in ["state", "disease", "district"]:
            df[col] = df[col].str.strip().str.title()
        df.sort_values(["year", "week"], inplace=True)

        p_main = f"{OUTPUT_DIR}/idsp_outbreaks_2013_2025.csv"
        df.to_csv(p_main, index=False)
        print(f"\n  Main dataset  → {p_main}")
        print(f"  Rows: {len(df):,}  |  Diseases: {df['disease'].nunique()}  "
              f"|  States: {df['state'].nunique()}")

        summary = (
            df.groupby("year").agg(
                outbreaks    = ("disease", "count"),
                total_cases  = ("cases",   "sum"),
                total_deaths = ("deaths",  "sum"),
                diseases     = ("disease", "nunique"),
                states       = ("state",   "nunique"),
            ).reset_index()
        )
        p_sum = f"{OUTPUT_DIR}/idsp_yearly_summary.csv"
        summary.to_csv(p_sum, index=False)
        print(f"  Year summary  → {p_sum}\n")
        print(summary.to_string(index=False))
    else:
        print("\n  ⚠  No records extracted — check the download log.")

    p_log = f"{OUTPUT_DIR}/idsp_download_log.csv"
    pd.DataFrame(log_rows).to_csv(p_log, index=False)
    print(f"\n  Download log  → {p_log}")
    print("\n✅  Done.")


if __name__ == "__main__":
    main()

# it automates:

# Collecting PDF links (2013–2025)
# Downloading each PDF
# Extracting outbreak data (tables or text)
# Cleaning + structuring the data
# Saving outputs as CSV files

# Final outputs:

# Full outbreak dataset
# Year-wise summary
# Download log
# Master list of PDF URLs