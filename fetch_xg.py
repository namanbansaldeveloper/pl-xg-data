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

PLAYER_STATS = ["xG", "npxG", "xA", "key_passes", "shots"]


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
    Fetch all PL players, then for each player fetch their match logs
    to compute last-6-GW and last-GW totals.

    Understat's get_league_players gives season totals.
    We then get per-match logs for each player to slice last 6 / last 1.
    """
    print("\nFetching player season stats...")
    players = await understat.get_league_players("epl", SEASON)
    print(f"  {len(players)} players found")

    # Build a sorted list of all unique match dates across all teams
    all_dates = sorted(set(
        m["date"]
        for matches in team_xg_data.values()
        for m in matches
    ))

    # Last GW = most recent date
    last_gw_date = all_dates[-1] if all_dates else None
    # Last 6 GWs = last 6 unique dates
    last_6_dates = set(all_dates[-6:]) if len(all_dates) >= 6 else set(all_dates)

    # Also compute cutoff timestamps for fuzzy matching
    # (player logs use full datetime strings, not just dates)
    last_gw_cutoff = last_gw_date  # players within this date string
    last_6_cutoff  = min(last_6_dates) if last_6_dates else last_gw_date

    print(f"  Last GW date  : {last_gw_date}")
    print(f"  Last 6 cutoff : {last_6_cutoff} → {last_gw_date}")

    print("\nFetching per-player match logs (this takes ~1-2 min)...")
    player_rows = []

    for i, p in enumerate(players):
        pid   = p["id"]
        pname = p["player_name"]
        team  = p["team_title"]

        # Only process PL teams
        if team not in PL_TEAMS:
            continue

        try:
            logs = await understat.get_player_matches(pid, {"season": SEASON})
        except Exception as e:
            print(f"  ⚠ Could not fetch logs for {pname}: {e}")
            continue

        # Filter to EPL only
        epl_logs = [m for m in logs if m.get("league") == "EPL"]

        def sum_stat(log_list, stat):
            return round(sum(float(m.get(stat, 0) or 0) for m in log_list), 2)

        # Use date string prefix for matching (handles timezone differences)
        # Player log dates look like "2025-11-02 15:00:00" — we take first 10 chars
        def log_date(m):
            return (m.get("date") or "")[:10]

        # Last 6 GWs: any match on or after the earliest of the last 6 GW dates
        l6 = [m for m in epl_logs if last_6_cutoff <= log_date(m) <= last_gw_date]
        # Last GW: any match on the last GW date
        l1 = [m for m in epl_logs if log_date(m) == last_gw_date]

        # Debug first few players so we can verify in Actions log
        if i < 3:
            print(f"  {pname}: {len(epl_logs)} EPL logs, l6={len(l6)}, l1={len(l1)}")

        player_rows.append({            "id":     pid,
            "name":   pname,
            "team":   team,
            # Season totals (from league players endpoint)
            "season": {
                "xG":        round(float(p.get("xG",  0) or 0), 2),
                "npxG":      round(float(p.get("npxG", 0) or 0), 2),
                "xA":        round(float(p.get("xA",  0) or 0), 2),
                "shots":     int(p.get("shots", 0) or 0),
                "key_passes": int(p.get("key_passes", 0) or 0),
                "goals":     int(p.get("goals", 0) or 0),
                "assists":   int(p.get("assists", 0) or 0),
                "games":     int(p.get("games", 0) or 0),
            },
            # Last 6 GWs
            "last6": {
                "xG":         sum_stat(l6, "xG"),
                "npxG":       sum_stat(l6, "npxG"),
                "xA":         sum_stat(l6, "xA"),
                "shots":      int(sum(int(m.get("shots", 0) or 0) for m in l6)),
                "key_passes": int(sum(int(m.get("key_passes", 0) or 0) for m in l6)),
                "goals":      int(sum(int(m.get("goals", 0) or 0) for m in l6)),
                "assists":    int(sum(int(m.get("assists", 0) or 0) for m in l6)),
                "games":      len(l6),
            },
            # Last GW
            "lastGW": {
                "xG":         sum_stat(l1, "xG"),
                "npxG":       sum_stat(l1, "npxG"),
                "xA":         sum_stat(l1, "xA"),
                "shots":      int(sum(int(m.get("shots", 0) or 0) for m in l1)),
                "key_passes": int(sum(int(m.get("key_passes", 0) or 0) for m in l1)),
                "goals":      int(sum(int(m.get("goals", 0) or 0) for m in l1)),
                "assists":    int(sum(int(m.get("assists", 0) or 0) for m in l1)),
                "games":      len(l1),
            },
        })

        if (i + 1) % 20 == 0:
            print(f"  Processed {i+1} players...")

    print(f"  Done — {len(player_rows)} PL players processed")
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

        # 1. Team match-by-match xG
        team_xg = await fetch_team_xg(understat)

        # 2. Player stats (last 6 GWs + last GW)
        players = await fetch_player_stats(understat, team_xg)

    # Bundle everything into one JSON
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
