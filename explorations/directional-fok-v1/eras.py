"""Stage B required test eras.

These windows are the generalization set. The Sep 2026 freeze weekend is
not enough. Families stay frozen; we re-fit the two HGBs inside each era
and transfer across eras. Polymarket volume and bot share rose through
this span — that is the point of the test, not a reason to drop old tape.

UTC date bounds are inclusive on the start day 00:00 and exclusive on the
end day 00:00 unless noted. Block numbers are filled by resolve_era_blocks
(Polygon RPC) and written to era-blocks.json.
"""

from __future__ import annotations

# (id, category, utc_start, utc_end, title, why)
# utc_* are ISO dates YYYY-MM-DD (00:00 UTC). end is exclusive.
ERAS: list[dict] = [
    # --- FIFA / football ---
    {
        "id": "fifa_wc_2022",
        "category": "fifa",
        "start": "2022-11-22",
        "end": "2022-12-19",
        "title": "FIFA World Cup 2022 (Qatar)",
        "why": "Tournament 20 Nov–18 Dec 2022. Fills start 22 Nov, so this is the first sports mega-event on our tape, thin book.",
    },
    {
        "id": "euro_copa_2024",
        "category": "fifa",
        "start": "2024-06-14",
        "end": "2024-07-15",
        "title": "Euro 2024 + Copa América 2024",
        "why": "UEFA Euro 14 Jun–14 Jul 2024 and Copa América 20 Jun–14 Jul 2024. Overlaps UK general election 4 Jul 2024.",
    },
    {
        "id": "cwc_2025",
        "category": "fifa",
        "start": "2025-06-14",
        "end": "2025-07-14",
        "title": "FIFA Club World Cup 2025 (USA)",
        "why": "Expanded 32-club CWC on US soil, 14 Jun–13 Jul 2025. Dress rehearsal for WC 2026; US-hours sports tape.",
    },
    {
        "id": "fifa_wc_2026",
        "category": "fifa",
        "start": "2026-06-11",
        "end": "2026-07-20",
        "title": "FIFA World Cup 2026 (USA/Mexico/Canada)",
        "why": "11 Jun–19 Jul 2026. Largest sports event on this tape. Overlaps July 2026 Hormuz/Iran strike cycle — documented, not dropped.",
    },
    # --- Super Bowl ---
    {
        "id": "sb_2023",
        "category": "super_bowl",
        "start": "2023-02-05",
        "end": "2023-02-14",
        "title": "Super Bowl LVII week",
        "why": "Game 12 Feb 2023 (Chiefs–Eagles). Window is conference championship week through the Monday after.",
    },
    {
        "id": "sb_2024",
        "category": "super_bowl",
        "start": "2024-02-04",
        "end": "2024-02-13",
        "title": "Super Bowl LVIII week",
        "why": "Game 11 Feb 2024 (Chiefs–49ers).",
    },
    {
        "id": "sb_2025",
        "category": "super_bowl",
        "start": "2025-02-02",
        "end": "2025-02-11",
        "title": "Super Bowl LIX week",
        "why": "Game 9 Feb 2025 (Eagles–Chiefs).",
    },
    {
        "id": "sb_2026",
        "category": "super_bowl",
        "start": "2026-02-01",
        "end": "2026-02-10",
        "title": "Super Bowl LX week",
        "why": "Game 8 Feb 2026 (Seahawks–Patriots). Closest Super Bowl to the freeze weekend; higher bot share expected.",
    },
    # --- US election / political ---
    {
        "id": "ticket_shock_2024",
        "category": "election",
        "start": "2024-07-13",
        "end": "2024-07-29",
        "title": "Butler attempt + Biden withdrawal",
        "why": "13 Jul 2024 assassination attempt; 21 Jul Biden drops out. Intra-campaign shock, not Election Day.",
    },
    {
        "id": "us_election_2024",
        "category": "election",
        "start": "2024-10-15",
        "end": "2024-11-13",
        "title": "US presidential election 2024",
        "why": "Election Day 5 Nov 2024, plus the last three campaign weeks and the call (6 Nov). Highest political volume on Polymarket in this span.",
    },
    {
        "id": "inauguration_2025",
        "category": "election",
        "start": "2025-01-13",
        "end": "2025-01-28",
        "title": "US inauguration 2025",
        "why": "20 Jan 2025 swearing-in and the surrounding executive-order tape.",
    },
    # --- US military intervention: before / after ---
    {
        "id": "oct7_before",
        "category": "us_military",
        "start": "2023-09-23",
        "end": "2023-10-07",
        "title": "Before 7 Oct 2023",
        "why": "Two weeks before Hamas’s attack and the US support/escalation that followed.",
    },
    {
        "id": "oct7_after",
        "category": "us_military",
        "start": "2023-10-07",
        "end": "2023-10-22",
        "title": "After 7 Oct 2023",
        "why": "First two weeks of the Gaza war and US military/diplomatic surge.",
    },
    {
        "id": "yemen_before",
        "category": "us_military",
        "start": "2023-12-29",
        "end": "2024-01-12",
        "title": "Before US–UK Yemen strikes",
        "why": "Two weeks before Operation Poseidon Archer (first strikes 12 Jan 2024).",
    },
    {
        "id": "yemen_after",
        "category": "us_military",
        "start": "2024-01-12",
        "end": "2024-01-27",
        "title": "US–UK Yemen strikes start",
        "why": "First two weeks of the January 2024 Red Sea campaign.",
    },
    {
        "id": "rough_rider_before",
        "category": "us_military",
        "start": "2025-03-01",
        "end": "2025-03-15",
        "title": "Before Operation Rough Rider",
        "why": "Two weeks before Trump-term Houthi campaign (15 Mar 2025).",
    },
    {
        "id": "rough_rider_after",
        "category": "us_military",
        "start": "2025-03-15",
        "end": "2025-03-30",
        "title": "Operation Rough Rider start",
        "why": "First two weeks of the 15 Mar–6 May 2025 Yemen campaign.",
    },
    {
        "id": "iran_before",
        "category": "us_military",
        "start": "2026-02-14",
        "end": "2026-02-28",
        "title": "Before 2026 Iran war",
        "why": "Two weeks before 28 Feb 2026 US–Israel opening strikes (Khamenei killed that day).",
    },
    {
        "id": "iran_after",
        "category": "us_military",
        "start": "2026-02-28",
        "end": "2026-03-15",
        "title": "2026 Iran war opening",
        "why": "First two weeks of the 2026 Iran war / Operation Epic Fury.",
    },
    # --- Quiet controls (popularity/bots without a mega-event) ---
    {
        "id": "quiet_aug_2023",
        "category": "quiet",
        "start": "2023-08-14",
        "end": "2023-08-29",
        "title": "Quiet mid-2023",
        "why": "No World Cup, Super Bowl, or US election. Early-venue control.",
    },
    {
        "id": "quiet_aug_2025",
        "category": "quiet",
        "start": "2025-08-11",
        "end": "2025-08-26",
        "title": "Quiet mid-2025",
        "why": "After CWC 2025, before 2026 cycle. Later-venue control (more users/bots).",
    },
    # --- Original freeze weekend (busy recent tape) ---
    {
        "id": "recent_sep2026",
        "category": "recent",
        "start": "2026-09-11",
        "end": "2026-09-15",
        "title": "Freeze weekend (Sep 2026)",
        "why": "The original Stage B fit window. Highest bot density we have; not a hold-out from itself.",
    },
]

REQUIRED_CATEGORIES = ("election", "super_bowl", "fifa", "us_military")


def era_by_id(eid: str) -> dict:
    for e in ERAS:
        if e["id"] == eid:
            return e
    raise KeyError(eid)
