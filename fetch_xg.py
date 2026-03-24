# fetch_xg.py v2
# Fetches:
#   - PL 2025/26 match-by-match xG per team (all 20 teams)
#   - Player stats: xG, npxG, xA, shots, key passes — per match for last 6 GWs + last GW
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
    For each PL team, fetch players via get_team_players which returns
    per-player season history with match-level xG data we can slice.
    """
    print("\nFetching player stats per team...")

    # Get sorted list of all unique match dates
    all_dates = sorted(set(
        m["date"]
        for matches in team_xg_data.values()
        for m in matches
    ))

    last_gw_date  = all_dates[-1] if all_dates else None
    last_6_dates  = set(all_dates[-6:]) if len(all_dates) >= 6 else set(all_dates)
    last_6_cutoff = min(last_6_dates) if last_6_dates else last_gw_date

    print(f"  Last GW date  : {last_gw_date}")
    print(f"  Last 6 cutoff : {last_6_cutoff} → {last_gw_date}")

    def log_date(m):
        return (m.get("date") or "")[:10]

    def safe_float(v):
        try: return float(v or 0)
        except: return 0.0

    def safe_int(v):
        try: return int(v or 0)
        except: return 0

    # Accumulate stats across all teams keyed by player id
    player_map = {}

    for team_name in PL_TEAMS:
        print(f"  Fetching players for {team_name}...")
        try:
            team_players = await understat.get_team_players(team_name, SEASON)
        except Exception as e:
            print(f"    ⚠ Failed: {e}")
            continue

        print(f"    → {len(team_players)} players, first player keys: {list(team_players[0].keys()) if team_players else '[]'}")

        for p in team_players:
            pid   = str(p.get("id", ""))
            pname = p.get("player_name") or p.get("name", "unknown")
            matches = p.get("matches", p.get("history", []))

            # Debug first player per team
            if team_players.index(p) == 0:
                print(f"    Sample player: {pname}, matches type: {type(matches)}, count: {len(matches) if isinstance(matches, list) else 'N/A'}")
                if matches and isinstance(matches, list):
                    print(f"    Sample match keys: {list(matches[0].keys())}")
                    print(f"    Sample match dates: {[log_date(m) for m in matches[:3]]}")

            if pid not in player_map:
                player_map[pid] = {
                    "id":   pid,
                    "name": pname,
                    "team": team_name,
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
                    "last6":  {"xG":0.0,"npxG":0.0,"xA":0.0,"shots":0,"key_passes":0,"goals":0,"assists":0,"games":0},
                    "lastGW": {"xG":0.0,"npxG":0.0,"xA":0.0,"shots":0,"key_passes":0,"goals":0,"assists":0,"games":0},
                }

            if isinstance(matches, list):
                for m in matches:
                    d = log_date(m)
                    if not d:
                        continue
                    in_l6 = last_6_cutoff <= d <= last_gw_date
                    in_l1 = d == last_gw_date

                    for bucket, include in [("last6", in_l6), ("lastGW", in_l1)]:
                        if not include:
                            continue
                        r = player_map[pid][bucket]
                        r["xG"]         = round(r["xG"]   + safe_float(m.get("xG")),   2)
                        r["npxG"]       = round(r["npxG"] + safe_float(m.get("npxG")),  2)
                        r["xA"]         = round(r["xA"]   + safe_float(m.get("xA")),    2)
                        r["shots"]      += safe_int(m.get("shots"))
                        r["key_passes"] += safe_int(m.get("key_passes"))
                        r["goals"]      += safe_int(m.get("goals"))
                        r["assists"]    += safe_int(m.get("assists"))
                        r["games"]      += 1

    players = list(player_map.values())

    # Show top 5 by last 6 xG so we can verify in logs
    top5 = sorted(players, key=lambda x: x["last6"]["xG"], reverse=True)[:5]
    print("\n  Top 5 by last 6 xG:")
    for p in top5:
        print(f"    {p['name']} ({p['team']}): last6 xG={p['last6']['xG']}, lastGW xG={p['lastGW']['xG']}, games={p['last6']['games']}")

    print(f"\n  Done — {len(players)} players processed")
    return players


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

        # 1. Team match-by-match xG
        team_xg = await fetch_team_xg(understat)

        # 2. Player stats (last 6 GWs + last GW)
        players = await fetch_player_stats(understat, team_xg)

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
