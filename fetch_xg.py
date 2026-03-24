import asyncio
import json
import os
import aiohttp
from understat import Understat
from datetime import datetime, timezone

# --- CONFIGURATION (2025/26 Season) ---
SEASON = "2025"
PL_TEAMS = [
    "Arsenal", "Aston Villa", "Bournemouth", "Brentford", "Brighton",
    "Burnley", "Chelsea", "Crystal Palace", "Everton", "Fulham",
    "Leeds United", "Liverpool", "Manchester City", "Manchester United",
    "Newcastle United", "Nottingham Forest", "Sunderland", "Tottenham",
    "West Ham", "Wolverhampton Wanderers"
]

async def fetch_team_xg(understat):
    print("Fetching team xG data...")
    teams_data = await understat.get_teams("epl", SEASON)
    all_data = {}
    for team in teams_data:
        name = team["title"]
        if name not in PL_TEAMS: continue
        matches = []
        for m in team["history"]:
            matches.append({
                "date": m["date"][:10],
                "xg": round(float(m["xG"]), 2),
                "xgc": round(float(m["xGA"]), 2),
                "goals": int(m["scored"]),
                "missed": int(m["missed"]),
                "result": m["result"],
                "home": m["h_a"] == "h",
            })
        matches.sort(key=lambda x: x["date"])
        all_data[name] = matches
    return all_data

async def fetch_player_stats(understat):
    print("\nFetching player season stats...")
    players = await understat.get_league_players("epl", SEASON)
    sorted_players = sorted(players, key=lambda x: float(x.get("xG", 0)), reverse=True)
    player_rows = []

    for i, p in enumerate(sorted_players[:150]):
        pid, pname, team = p["id"], p["player_name"], p["team_title"]
        if team not in PL_TEAMS: continue

        try:
            logs = await understat.get_player_matches(pid)
        except Exception: continue

        epl_logs = []
        for m in logs:
            m_date = m.get("date", "")
            is_current = "2025-" in m_date or "2026-" in m_date
            is_pl = m.get("h_team") in PL_TEAMS or m.get("a_team") in PL_TEAMS
            if is_current and is_pl: epl_logs.append(m)

        epl_logs.sort(key=lambda x: x["date"], reverse=True)
        l1, l6 = epl_logs[:1], epl_logs[:6]

        def s_st(lst, st):
            return round(sum(float(m.get(st, 0) or 0) for m in lst), 2)

        player_rows.append({
            "id": pid, "name": pname, "team": team,
            "season": {
                "xG": round(float(p.get("xG", 0)), 2),
                "xA": round(float(p.get("xA", 0)), 2),
                "goals": int(p.get("goals", 0)),
                "assists": int(p.get("assists", 0)),
                "games": int(p.get("games", 0)),
            },
            "last6": {
                "xG": s_st(l6, "xG"), "xA": s_st(l6, "xA"),
                "goals": int(sum(int(m.get("goals", 0)) for m in l6)),
                "games": len(l6),
            },
            "lastGW": {
                "xG": s_st(l1, "xG"), "xA": s_st(l1, "xA"),
                "goals": int(sum(int(m.get("goals", 0)) for m in l1)),
                "date": l1[0]["date"] if l1 else "N/A"
            }
        })
        if (i + 1) % 50 == 0: print(f"  Processed {i+1} players...")
    return player_rows

async def upload_to_gist(data):
    g_id = os.environ["GIST_ID"]
