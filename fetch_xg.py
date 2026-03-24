# fetch_xg.py v5
# Uses get_league_results to find match IDs, then get_match_players
# to aggregate per-player stats for last 6 matches and last GW.
#
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

    # ── Step 1: season totals from league players ──────────────────────────
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

    # ── Step 2: get all league results to find match IDs + dates ──────────
    print("  Fetching all league results for match IDs...")
    results = await understat.get_league_results("epl", SEASON)

    # Sort by date, keep only played matches
    results = [r for r in results if r.get("isResult")]
    results.sort(key=lambda r: r.get("datetime", ""))

    print(f"  {len(results)} completed matches found")
    if results:
        print(f"  Date range: {results[0]['datetime'][:10]} → {results[-1]['datetime'][:10]}")

    # Get unique match dates to find last 6 GW dates
    all_dates = sorted(set(r["datetime"][:10] for r in results))
    last_gw_date  = all_dates[-1] if all_dates else ""
    last_6_dates  = set(all_dates[-6:]) if len(all_dates) >= 6 else set(all_dates)

    print(f"  Last GW date : {last_gw_date}")
    print(f"  Last 6 dates : {sorted(last_6_dates)}")

    # Get match IDs for last 6 GWs and last GW
    last6_ids = [r["id"] for r in results if r["datetime"][:10] in last_6_dates]
    last1_ids = [r["id"] for r in results if r["datetime"][:10] == last_gw_date]

    print(f"  Matches in last 6 GWs: {len(last6_ids)}")
    print(f"  Matches in last GW:    {len(last1_ids)}")

    # ── Step 3: fetch player stats for each match ─────────────────────────
    # player_buckets[pid][bucket] = accumulated stats dict
    player_buckets = {}  # pid -> {"last6": {...}, "lastGW": {...}}

    debug_done = False
    async def process_matches(match_ids, bucket_name):
        nonlocal debug_done
        print(f"  Fetching player data for {len(match_ids)} matches ({bucket_name})...")
        for mid in match_ids:
            try:
                match_players = await understat.get_match_players(mid)
                # Debug: print raw structure of first match only
                if not debug_done:
                    debug_done = True
                    print(f"\n  DEBUG match {mid} raw type: {type(match_players)}")
                    if isinstance(match_players, dict):
                        print(f"  DEBUG top-level keys: {list(match_players.keys())}")
                        for side in list(match_players.keys())[:1]:
                            side_data = match_players[side]
                            print(f"  DEBUG side '{side}' type: {type(side_data)}")
                            if isinstance(side_data, dict):
                                first_pid = list(side_data.keys())[0] if side_data else None
                                if first_pid:
                                    print(f"  DEBUG first player id: {first_pid}")
                                    print(f"  DEBUG first player data: {side_data[first_pid]}")
                            elif isinstance(side_data, list) and side_data:
                                print(f"  DEBUG first item: {side_data[0]}")
                    elif isinstance(match_players, list) and match_players:
                        print(f"  DEBUG first item: {match_players[0]}")
                    print()
                # get_match_players returns {"h": {pid: {...}}, "a": {pid: {...}}}
                for side in ("h", "a"):
                    side_data = match_players.get(side, {})
                    for pid, pdata in side_data.items():
                        pid = str(pid)
                        if pid not in season_totals:
                            continue
                        if pid not in player_buckets:
                            player_buckets[pid] = {
                                "last6":  {"xG":0.0,"npxG":0.0,"xA":0.0,"shots":0,"key_passes":0,"goals":0,"assists":0,"games":0},
                                "lastGW": {"xG":0.0,"npxG":0.0,"xA":0.0,"shots":0,"key_passes":0,"goals":0,"assists":0,"games":0},
                            }
                        b = player_buckets[pid][bucket_name]
                        b["xG"]         = round(b["xG"]   + safe_float(pdata.get("xG")),   2)
                        b["npxG"]       = round(b["npxG"] + safe_float(pdata.get("npxG")),  2)
                        b["xA"]         = round(b["xA"]   + safe_float(pdata.get("xA")),    2)
                        b["shots"]      += safe_int(pdata.get("shots"))
                        b["key_passes"] += safe_int(pdata.get("key_passes"))
                        b["goals"]      += safe_int(pdata.get("goals"))
                        b["assists"]    += safe_int(pdata.get("assists"))
                        b["games"]      += 1
            except Exception as e:
                print(f"    ⚠ match {mid} failed: {e}")

    # Fetch last 6 first (includes last GW matches)
    await process_matches(last6_ids, "last6")
    # Fetch last GW separately
    await process_matches(last1_ids, "lastGW")

    # ── Step 4: build final player rows ───────────────────────────────────
    player_rows = []
    for pid, s in season_totals.items():
        buckets = player_buckets.get(pid, {
            "last6":  {"xG":0.0,"npxG":0.0,"xA":0.0,"shots":0,"key_passes":0,"goals":0,"assists":0,"games":0},
            "lastGW": {"xG":0.0,"npxG":0.0,"xA":0.0,"shots":0,"key_passes":0,"goals":0,"assists":0,"games":0},
        })
        player_rows.append({
            "id":     pid,
            "name":   s["name"],
            "team":   s["team"],
            "season": {k: s[k] for k in ("xG","npxG","xA","shots","key_passes","goals","assists","games")},
            "last6":  buckets["last6"],
            "lastGW": buckets["lastGW"],
        })

    # Show top 5 by last 6 xG
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
        async with session.patch(f"https://api.github.com/gists/{gist_id}", json=payload, headers=headers) as resp:
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
        "meta": {"updated_at": datetime.now(timezone.utc).isoformat(), "season": f"{SEASON}/{int(SEASON)+1}"},
        "teams":   team_xg,
        "players": players,
    }
    print(f"\nUploading to Gist...")
    await upload_to_gist(output)


if __name__ == "__main__":
    asyncio.run(main())
