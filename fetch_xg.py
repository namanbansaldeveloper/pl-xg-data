    import asyncio
import json
import os
import aiohttp
from understat import Understat
from datetime import datetime, timezone

# --- CONFIGURATION (2025/26 Season) ---
SEASON = "2025"  # Understat uses the start year for the 2025/26 season
PL_TEAMS = [
    "Arsenal", "Aston Villa", "Bournemouth", "Brentford", "Brighton",
    "Burnley", "Chelsea", "Crystal Palace", "Everton", "Fulham",
    "Leeds United", "Liverpool", "Manchester City", "Manchester United",
    "Newcastle United", "Nottingham Forest", "Sunderland", "Tottenham",
    "West Ham", "Wolverhampton Wanderers"
]

async def fetch_team_xg(understat):
    """Fetch match-by-match xG for the 2025/26 PL teams."""
    print("Fetching team xG data...")
    teams_data = await understat.get_teams("epl", SEASON)
    all_data = {}
    for team in teams_data:
        name = team["title"]
        if name not in PL_TEAMS:
            continue
        matches = []
        for m in team["history"]:
            matches.append({
                "date":   m["date"][:10],
                "xg":     round(float(m["xG"]),  2),
                "xgc":    round(float(m["xGA"]), 2),
                "goals":  int(m["scored"]),
                "missed": int(m["missed"]),
                "result": m["result"],
                "home":   m["h_a"] == "h",
            })
        matches.sort(key=lambda x: x["date"])
        all_data[name] = matches
    return all_data

async def fetch_player_stats(understat):
    """Fetch 2025/26 players and their 'Recent Form' match logs."""
    print("\nFetching player season stats...")
    players = await understat.get_league_players("epl", SEASON)
    
    # Process top 150 by xG to keep the slide deck data focused and fast
    sorted_players = sorted(players, key=lambda x: float(x.get("xG", 0)), reverse=True)
    player_rows = []

    for i, p in enumerate(sorted_players[:150]):
        pid, pname, team = p["id"], p["player_name"], p["team_title"]
        if team not in PL_TEAMS:
            continue

        try:
            # Fetch match logs (no season filter here to avoid library bugs)
            logs = await understat.get_player_matches(pid)
        except Exception as e:
            print(f"  ⚠ Skip {pname}: {e}")
            continue

        # Filter logs specifically for the 2025/26 window
        epl_logs = []
        for m in logs:
            m_date = m.get("date", "")
            # Matches in 25/26 happen in 2025 or 2026
            is_current_season = "2025-" in m_date or "2026-" in m_date
            is_pl_match = m.get("h_team") in PL_TEAMS or m.get("a_team") in PL_TEAMS
            
            if is_current_season and is_pl_match:
                epl_logs.append(m)

        # Newest first
        epl_logs.sort(key=lambda x: x["date"], reverse=True)
        l1 = epl_logs[:1]
        l6 = epl_logs[:6]

        def sum_stat(log_list, stat):
            return round(sum(float(m.get(stat, 0) or 0) for m in log_list), 2)

        player_rows.append({
            "id": pid,
            "name": pname,
            "team": team,
            "season": {
                "x
