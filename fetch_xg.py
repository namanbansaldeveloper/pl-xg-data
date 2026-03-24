# fetch_xg.py v3
# Fetches:
#   - PL 2025/26 match-by-match xG per team (all 20 teams)
#   - Player stats: xG, npxG, xA, shots, key passes — last 6 matches + last match
#
# Env vars required (GitHub Actions secrets):
#   GIST_ID    – ID of your GitHub Gist
#   GIST_TOKEN – GitHub personal access token with "gist" scope

import asyncio
import json
import os
import aiohttp
from understat import Understat
from datetime import datetime, timezone

SEASON = "2025"

PL_TEAMS = [
    "Arsenal", "Manchester City", "Liverpool", "Chelsea", "Tottenham",
    "Aston Villa", "Newcastle United", "Manchester United", "Brighton",
    "West Ham", "Brentford", "Crystal Palace", "Bournemouth", "Fulham",
    "Everton", "Nottingham Forest", "Leeds", "Sunderland", "Burnley",
    "Wolverhampton Wanderers",
]


async def fetch_team_xg(understat):
    """Fetch match-by-match xG for all 20 PL teams."""
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
        print(f"  {name}: {len(matches)} matches")
    return all_data


async def fetch_player_stats(understat, team_xg_data):
    """
    Fetch all PL players then get per-match logs via get_player_matches.
    Slice the last 6 matches and last 1 match from each player's sorted log.
    """
    print("\nFetching player season stats...")
    all_players = await understat.get_league_players("epl", SEASON)
    print(f"  {len(all_players)} players found")

    def safe_float(v):
        try: return float(v or 0)
        except: return 0.0

    def safe_int(v):
        try: return int(v or 0)
        except: return 0

    def sum_stat(logs, stat):
        return round(sum(safe_float(m.get(stat)) for m in logs), 2)

    player_rows = []

    for i, p in enumerate(all_players):
        pid   = p["id"]
        pname = p["player_name"]
        team  = p["team_title"]

        if team not in PL_TEAMS:
            continue

        try:
            # get_player_matches returns ALL matches for this player across all seasons/leagues
            # We pass season filter to narrow it down
            logs = await understat.get_player_matches(pid, {"season": SEASON})
        except Exception as e:
            print(f"  ⚠ Could not fetch {pname}: {e}")
            continue

        # Sort by date ascending so last entries = most recent
        logs.sort(key=lambda m: m.get("date",""))

        # Debug first 2 players AND anyone named Malen
        if i < 2 or "malen" in pname.lower():
            print(f"  {pname}: {len(logs)} total logs")
            if logs:
                print(f"    ALL dates: {[m.get('date','?')[:10] for m in logs]}")
                print(f"    league values: {list(set(str(m.get('league') or m.get('h_a') or '?') for m in logs))}")
                print(f"    season values: {list(set(str(m.get('season','?')) for m in logs))}")

        # Filter to 2025/26 season only (on or after 2025-08-01)
        season_logs = [m for m in logs if (m.get("date") or "")[:7] >= "2025-08"]

        if i < 2 or "malen" in pname.lower():
            print(f"    → after filter: {len(season_logs)} logs, dates: {[m.get('date','?')[:10] for m in season_logs]}")

        # Slice last 6 and last 1 from THIS SEASON only
        last6 = season_logs[-6:] if len(season_logs) >= 6 else season_logs
        last1 = season_logs[-1:] if season_logs else []

        player_rows.append({
            "id":   pid,
            "name": pname,
            "team": team,
            "season": {
                "xG":         round(safe_float(p.get("xG")),   2),
                "npxG":       round(safe_float(p.get("npxG")),  2),
                "xA":         round(safe_float(p.get("xA")),    2),
                "shots":      safe_int(p.get("shots")),
                "key_passes": safe_int(p.get("key_passes")),
                "goals":      safe_int(p.get("goals")),
                "assists":    safe_int(p.get("assists")),
                "games":      safe_int(p.get("games")),
            },
            "last6": {
                "xG":         sum_stat(last6, "xG"),
                "npxG":       sum_stat(last6, "npxG"),
                "xA":         sum_stat(last6, "xA"),
                "shots":      sum(safe_int(m.get("shots"))      for m in last6),
                "key_passes": sum(safe_int(m.get("key_passes")) for m in last6),
                "goals":      sum(safe_int(m.get("goals"))      for m in last6),
                "assists":    sum(safe_int(m.get("assists"))     for m in last6),
                "games":      len(last6),
            },
            "lastGW": {
                "xG":         sum_stat(last1, "xG"),
                "npxG":       sum_stat(last1, "npxG"),
                "xA":         sum_stat(last1, "xA"),
                "shots":      sum(safe_int(m.get("shots"))      for m in last1),
                "key_passes": sum(safe_int(m.get("key_passes")) for m in last1),
                "goals":      sum(safe_int(m.get("goals"))      for m in last1),
                "assists":    sum(safe_int(m.get("assists"))     for m in last1),
                "games":      len(last1),
            },
        })

        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1} players...")

    # Show top 5 by last 6 xG to verify
    top5 = sorted(player_rows, key=lambda x: x["last6"]["xG"], reverse=True)[:5]
    print("\n  Top 5 by last 6 xG:")
    for p in top5:
        print(f"    {p['name']} ({p['team']}): last6 xG={p['last6']['xG']} games={p['last6']['games']}, lastGW xG={p['lastGW']['xG']}")

    print(f"\n  Done — {len(player_rows)} PL players processed")
    return player_rows


async def upload_to_gist(data: dict):
    gist_id    = os.environ["GIST_ID"]
    gist_token = os.environ["GIST_TOKEN"]

    payload = {
        "description": f"PL 2025/26 xG data — updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "files": {
            "pl_xg_2025_26.json": {
                "content": json.dumps(data, indent=2)
            }
        }
    }

    headers = {
        "Authorization": f"token {gist_token}",
        "Accept": "application/vnd.github+json",
    }

    async with aiohttp.ClientSession() as session:
        async with session.patch(
            f"https://api.github.com/gists/{gist_id}",
            json=payload,
            headers=headers,
        ) as resp:
            if resp.status == 200:
                print(f"\n✅ Gist updated successfully!")
            else:
                text = await resp.text()
                raise RuntimeError(f"Gist update failed ({resp.status}): {text}")


async def main():
    print(f"=== PL {SEASON}/{int(SEASON)+1} xG Fetcher v2 ===\n")
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        team_xg = await fetch_team_xg(understat)
        players  = await fetch_player_stats(understat, team_xg)

    output = {
        "meta": {
            "updated_at": datetime.now(timezone.utc).isoformat(),
            "season": f"{SEASON}/{int(SEASON)+1}",
        },
        "teams":   team_xg,
        "players": players,
    }

    print(f"\nUploading to Gist...")
    await upload_to_gist(output)


if __name__ == "__main__":
    asyncio.run(main())
