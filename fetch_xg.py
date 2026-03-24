import asyncio
import json
import aiohttp
from understat import Understat

# --- CONFIGURATION ---
SEASON = "2025"  # Understat uses the start year (2025-26 season)
PL_TEAMS = [
    "Arsenal", "Aston Villa", "Bournemouth", "Brentford", "Brighton",
    "Chelsea", "Crystal Palace", "Everton", "Fulham", "Ipswich",
    "Leicester", "Liverpool", "Manchester City", "Manchester United",
    "Newcastle United", "Nottingham Forest", "Southampton", "Tottenham",
    "West Ham", "Wolverhampton Wanderers"
]

async def fetch_player_stats(understat):
    print(f"Pulling PL Player Data for {SEASON}/26...")
    
    # 1. Get the high-level summary for all players in the league
    all_players = await understat.get_league_players("epl", SEASON)
    player_rows = []

    # Process players (limit to top performers to keep JSON manageable)
    # Sorting by xG so we get the most relevant players first
    sorted_players = sorted(all_players, key=lambda x: float(x.get("xG", 0)), reverse=True)

    for i, p in enumerate(sorted_players[:150]):  # Top 150 players for the slide deck
        pid = p["id"]
        pname = p["player_name"]
        team = p["team_title"]

        if team not in PL_TEAMS:
            continue

        try:
            # 2. Fetch ALL match logs for this specific player
            # We don't pass 'season' here to avoid API-side filtering bugs
            logs = await understat.get_player_matches(pid)
        except Exception as e:
            print(f"  ⚠ Skip {pname}: {e}")
            continue

        # 3. Manual Filter: Only matches from the current season and PL teams
        # Match logs usually have 'h_team'/'a_team' and a 'date'
        epl_logs = []
        for m in logs:
            match_date = m.get("date", "")
            # Matches in the 2025/26 season happen in 2025 or 2026
            is_current_season = "2025-" in match_date or "2026-" in match_date
            
            # Ensure it's a Premier League match (checking team names)
            is_pl = m.get("h_team") in PL_TEAMS or m.get("a_team") in PL_TEAMS
            
            if is_current_season and is_pl:
                epl_logs.append(m)

        # 4. Sort by date (Newest first) and Slice
        epl_logs.sort(key=lambda x: x["date"], reverse=True)
        l1 = epl_logs[:1]   # Last Gameweek
        l6 = epl_logs[:6]   # Last 6 Gameweeks

        def sum_stat(log_list, stat):
            return round(sum(float(m.get(stat, 0) or 0) for m in log_list), 2)

        player_rows.append({
            "id": pid,
            "name": pname,
            "team": team,
            "season_summary": {
                "xG": round(float(p.get("xG", 0)), 2),
                "xA": round(float(p.get("xA", 0)), 2),
                "goals": int(p.get("goals", 0)),
                "assists": int(p.get("assists", 0)),
                "games": int(p.get("games", 0))
            },
            "recent_form": {
                "last_6_xG": sum_stat(l6, "xG"),
                "last_6_xA": sum_stat(l6, "xA"),
                "last_6_goals": int(sum(int(m.get("goals", 0)) for m in l6)),
                "match_count": len(l6)
            },
            "last_gw": {
                "xG": sum_stat(l1, "xG"),
                "xA": sum_stat(l1, "xA"),
                "goals": int(sum(int(m.get("goals", 0)) for m in l1)),
                "date": l1[0]["date"] if l1 else "N/A"
            }
        })

        if (i + 1) % 25 == 0:
            print(f"  Processed {i+1} players...")

    return player_rows

async def main():
    async with aiohttp.ClientSession() as session:
