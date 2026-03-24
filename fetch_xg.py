# fetch_xg.py v4
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
    For each team, fetch player shots for the season.
    Group shots by (player, match_id) to get per-match xG totals.
    Then slice last 6 matches and last 1 match per player.
    """
    print("\nFetching player stats per team...")

    def safe_float(v):
        try: return float(v or 0)
        except: return 0.0

    def safe_int(v):
        try: return int(v or 0)
        except: return 0

    # Get season totals from league players endpoint (always accurate)
    all_players = await understat.get_league_players("epl", SEASON)
    season_totals = {}
    for p in all_players:
        if p["team_title"] in PL_TEAMS:
            season_totals[str(p["id"])] = {
                "name":       p["player_name"],
                "team":       p["team_title"],
                "xG":         round(safe_float(p.get("xG")),   2),
                "npxG":       round(safe_float(p.get("npxG")),  2),
                "xA":         round(safe_float(p.get("xA")),    2),
                "shots":      safe_int(p.get("shots")),
                "key_passes": safe_int(p.get("key_passes")),
                "goals":      safe_int(p.get("goals")),
                "assists":    safe_int(p.get("assists")),
                "games":      safe_int(p.get("games")),
            }
    print(f"  {len(season_totals)} PL players with season totals")

    # For each team, get all shots for the season
    # Each shot has: player_id, match_id, date, xG, result (Goal/Miss etc), situation
    # We group by (player_id, match_id) to get per-match aggregates

    # player_matches[player_id] = list of {date, match_id, xG, npxG, xA, shots, key_passes, goals, assists}
    player_matches = {}

    for team_name in PL_TEAMS:
        print(f"  Fetching shots for {team_name}...")
        try:
            shots = await understat.get_team_shots(team_name, SEASON)
        except Exception as e:
            print(f"    ⚠ Failed: {e}")
            continue

        print(f"    → {len(shots)} shots")
        if not shots:
            continue

        # Debug: show keys from first shot
        if team_name == "Arsenal":
            print(f"    Shot keys: {list(shots[0].keys())}")
            print(f"    Sample shot: {shots[0]}")

        # Group shots by player+match
        # shot fields include: player_id, match_id, date, xG, npxG (may not exist), result, situation
        match_groups = {}  # key: (player_id, match_id)
        for s in shots:
            pid = str(s.get("player_id", ""))
            mid = str(s.get("match_id",  ""))
            if not pid or not mid:
                continue
            key = (pid, mid)
            if key not in match_groups:
                match_groups[key] = {
                    "date":       (s.get("date") or "")[:10],
                    "xG":         0.0,
                    "npxG":       0.0,
                    "shots":      0,
                    "goals":      0,
                }
            g = match_groups[key]
            xg_val = safe_float(s.get("xG"))
            g["xG"]    = round(g["xG"] + xg_val, 4)
            # npxG = xG excluding penalties
            if s.get("situation") != "Penalty":
                g["npxG"] = round(g["npxG"] + xg_val, 4)
            g["shots"] += 1
            if s.get("result") == "Goal":
                g["goals"] += 1

        # Accumulate into player_matches
        for (pid, mid), data in match_groups.items():
            if pid not in player_matches:
                player_matches[pid] = []
            player_matches[pid].append(data)

    # Now fetch key passes (assists-level) separately via team results
    # Unfortunately Understat shot data doesn't include xA per shot easily,
    # so we use the season total xA from get_league_players (already in season_totals)
    # and approximate last6 xA by ratio: last6_shots/season_shots * season_xA
    # This is an approximation — noted in the UI

    print(f"\n  Building player records...")
    player_rows = []

    for pid, s in season_totals.items():
        matches_for_player = player_matches.get(pid, [])

        # Sort by date
        matches_for_player.sort(key=lambda m: m["date"])

        # Filter to this season just in case
        matches_for_player = [m for m in matches_for_player if m["date"] >= "2025-08-01"]

        last6 = matches_for_player[-6:] if len(matches_for_player) >= 6 else matches_for_player
        last1 = matches_for_player[-1:] if matches_for_player else []

        def sum_m(lst, k):
            return round(sum(safe_float(m.get(k, 0)) for m in lst), 2)

        # Approximate xA for last6/last1 by ratio
        season_xA    = s["xA"]
        season_shots = s["shots"] or 1
        l6_shots     = sum(safe_int(m.get("shots")) for m in last6)
        l1_shots     = sum(safe_int(m.get("shots")) for m in last1)
        l6_xA        = round(season_xA * (l6_shots / season_shots), 2)
        l1_xA        = round(season_xA * (l1_shots / season_shots), 2)

        player_rows.append({
            "id":   pid,
            "name": s["name"],
            "team": s["team"],
            "season": {
                "xG":         s["xG"],
                "npxG":       s["npxG"],
                "xA":         s["xA"],
                "shots":      s["shots"],
                "key_passes": s["key_passes"],
                "goals":      s["goals"],
                "assists":    s["assists"],
                "games":      s["games"],
            },
            "last6": {
                "xG":         sum_m(last6, "xG"),
                "npxG":       sum_m(last6, "npxG"),
                "xA":         l6_xA,
                "shots":      sum(safe_int(m.get("shots")) for m in last6),
                "key_passes": 0,  # not available at shot level
                "goals":      sum(safe_int(m.get("goals")) for m in last6),
                "assists":    0,
                "games":      len(last6),
            },
            "lastGW": {
                "xG":         sum_m(last1, "xG"),
                "npxG":       sum_m(last1, "npxG"),
                "xA":         l1_xA,
                "shots":      sum(safe_int(m.get("shots")) for m in last1),
                "key_passes": 0,
                "goals":      sum(safe_int(m.get("goals")) for m in last1),
                "assists":    0,
                "games":      len(last1),
            },
        })

    # Show top 5 by last 6 xG
    top5 = sorted(player_rows, key=lambda x: x["last6"]["xG"], reverse=True)[:5]
    print("\n  Top 5 by last 6 xG:")
    for p in top5:
        print(f"    {p['name']} ({p['team']}): xG={p['last6']['xG']} shots={p['last6']['shots']} games={p['last6']['games']}")

    print(f"\n  Done — {len(player_rows)} players")
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
    print(f"=== PL {SEASON}/{int(SEASON)+1} xG Fetcher v4 ===\n")
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
