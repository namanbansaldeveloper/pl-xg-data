# fetch_xg.py v5
# Env vars required:
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
    print("\nFetching player stats...")

    def safe_float(v):
        try: return float(v or 0)
        except: return 0.0

    def safe_int(v):
        try: return int(v or 0)
        except: return 0

    # ── Step 1: season totals ─────────────────────────────────────────────
    all_players = await understat.get_league_players("epl", SEASON)
    season_totals = {}
    for p in all_players:
        if p["team_title"] in PL_TEAMS:
            pid = str(p["id"])
            season_totals[pid] = {
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

    # ── Step 2: get all league results ────────────────────────────────────
    print("  Fetching league results...")
    results = [r for r in await understat.get_league_results("epl", SEASON) if r.get("isResult")]
    results.sort(key=lambda r: r.get("datetime", ""))
    print(f"  {len(results)} completed matches")

    # ── Step 3: build per-team match history ──────────────────────────────
    team_match_history = {}
    for r in results:
        date = r["datetime"][:10]
        for side in ("h", "a"):
            team_name = r[side]["title"]
            if team_name in PL_TEAMS:
                if team_name not in team_match_history:
                    team_match_history[team_name] = []
                team_match_history[team_name].append((r["id"], date))

    for team in team_match_history:
        team_match_history[team].sort(key=lambda x: x[1])

    # Last 6 and last 1 match IDs per team
    team_last6_ids = {t: [mid for mid, _ in v[-6:]] for t, v in team_match_history.items()}
    team_last1_ids = {t: [mid for mid, _ in v[-1:]] for t, v in team_match_history.items()}

    # All unique match IDs we need across all teams' last 6
    all_needed_ids = list(set(mid for ids in team_last6_ids.values() for mid in ids))
    print(f"  Unique matches to fetch: {len(all_needed_ids)}")

    for team in list(team_match_history.keys())[:2]:
        last6_dates = [d for _, d in team_match_history[team][-6:]]
        print(f"  {team} last 6 dates: {last6_dates}")

    # ── Step 4: fetch player data + shots for all needed matches ─────────
    match_player_data = {}  # match_id -> {player_id -> pdata}
    match_npxg_data   = {}  # match_id -> {player_id -> npxG float}

    print(f"  Fetching player data for {len(all_needed_ids)} matches...")
    for i, mid in enumerate(all_needed_ids):
        try:
            # Player stats (goals, shots, xA, key passes etc.)
            match_players = await understat.get_match_players(mid)
            match_player_data[mid] = {}
            for side in ("h", "a"):
                for _, pdata in match_players.get(side, {}).items():
                    pid = str(pdata.get("player_id", ""))
                    if pid:
                        match_player_data[mid][pid] = pdata

            # Shot data to compute npxG (exclude penalties)
            match_shots = await understat.get_match_shots(mid)
            match_npxg_data[mid] = {}
            for side in ("h", "a"):
                for shot in match_shots.get(side, []):
                    pid = str(shot.get("player_id", ""))
                    if not pid:
                        continue
                    if shot.get("situation") == "Penalty":
                        continue
                    xg_val = safe_float(shot.get("xG"))
                    match_npxg_data[mid][pid] = round(
                        match_npxg_data[mid].get(pid, 0.0) + xg_val, 4
                    )
        except Exception as e:
            print(f"    ⚠ match {mid} failed: {e}")
        if (i + 1) % 20 == 0:
            print(f"    {i+1}/{len(all_needed_ids)} fetched...")

    print("  Done fetching match data")

    # ── Step 5: aggregate per player ─────────────────────────────────────
    def agg(match_ids, pid):
        """Sum stats across match_ids. games = number of team matches in window."""
        result = {
            "xG": 0.0, "npxG": 0.0, "xA": 0.0,
            "shots": 0, "key_passes": 0,
            "goals": 0, "assists": 0,
            "games": len(match_ids),
        }
        for mid in match_ids:
            pdata = match_player_data.get(mid, {}).get(pid)
            if pdata:
                result["xG"]         = round(result["xG"] + safe_float(pdata.get("xG")), 2)
                result["npxG"]       = round(result["npxG"] + match_npxg_data.get(mid, {}).get(pid, safe_float(pdata.get("xG"))), 2)
                result["xA"]         = round(result["xA"] + safe_float(pdata.get("xA")), 2)
                result["shots"]      += safe_int(pdata.get("shots"))
                result["key_passes"] += safe_int(pdata.get("key_passes"))
                result["goals"]      += safe_int(pdata.get("goals"))
                result["assists"]    += safe_int(pdata.get("assists"))
        return result

    player_rows = []
    for pid, s in season_totals.items():
        team = s["team"]
        l6_ids = team_last6_ids.get(team, [])
        l1_ids = team_last1_ids.get(team, [])
        player_rows.append({
            "id":     pid,
            "name":   s["name"],
            "team":   team,
            "season": {k: s[k] for k in ("xG","npxG","xA","shots","key_passes","goals","assists","games")},
            "last6":  agg(l6_ids, pid),
            "lastGW": agg(l1_ids, pid),
        })

    top5 = sorted(player_rows, key=lambda x: x["last6"]["xG"], reverse=True)[:5]
    print("\n  Top 5 by last 6 xG:")
    for p in top5:
        print(f"    {p['name']} ({p['team']}): xG={p['last6']['xG']} games={p['last6']['games']} lastGW={p['lastGW']['xG']}")

    print(f"\n  Done — {len(player_rows)} players")
    return player_rows


async def upload_to_gist(data: dict):
    gist_id    = os.environ["GIST_ID"]
    gist_token = os.environ["GIST_TOKEN"]
    payload = {
        "description": f"PL 2025/26 xG data — updated {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
        "files": {"pl_xg_2025_26.json": {"content": json.dumps(data, indent=2)}}
    }
    headers = {"Authorization": f"token {gist_token}", "Accept": "application/vnd.github+json"}
    async with aiohttp.ClientSession() as session:
        async with session.patch(
            f"https://api.github.com/gists/{gist_id}",
            json=payload, headers=headers
        ) as resp:
            if resp.status == 200:
                print(f"\n✅ Gist updated successfully!")
            else:
                text = await resp.text()
                raise RuntimeError(f"Gist update failed ({resp.status}): {text}")


async def main():
    print(f"=== PL {SEASON}/{int(SEASON)+1} xG Fetcher v5 ===\n")
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
