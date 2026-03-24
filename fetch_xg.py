# fetch_xg.py
# Fetches PL 2025/26 match-by-match xG data from Understat
# and uploads it to a GitHub Gist as JSON.
#
# Required env variables (set as GitHub Actions secrets):
#   GIST_ID    – the ID of your GitHub Gist (create one manually first)
#   GIST_TOKEN – a GitHub personal access token with the "gist" scope

import asyncio
import json
import os
import aiohttp
from understat import Understat

SEASON = "2025"

TEAMS = [
    "Arsenal",
    "Manchester City",
    "Liverpool",
    "Chelsea",
    "Tottenham",
    "Aston Villa",
    "Newcastle United",
    "Manchester United",
    "Brighton",
    "West Ham",
    "Brentford",
    "Crystal Palace",
    "Bournemouth",
    "Fulham",
    "Everton",
    "Nottingham Forest",
    "Leeds",
    "Sunderland",
    "Burnley",
    "Wolverhampton Wanderers",
]


async def fetch_all(session):
    understat = Understat(session)
    all_data = {}

    # get_teams returns all 20 PL teams in one request — efficient!
    teams_data = await understat.get_teams("epl", SEASON)

    for team in teams_data:
        name = team["title"]
        if name not in TEAMS:
            continue
        matches = []
        for m in team["history"]:
            matches.append({
                "date":   m["date"][:10],
                "xg":     round(float(m["xG"]),   2),
                "xgc":    round(float(m["xGA"]),  2),
                "goals":  int(m["scored"]),
                "missed": int(m["missed"]),
                "result": m["result"],       # "w", "d", "l"
                "home":   m["h_a"] == "h",
            })
        # sort chronologically (they usually are, but just in case)
        matches.sort(key=lambda x: x["date"])
        all_data[name] = matches
        print(f"  {name}: {len(matches)} matches")

    return all_data


async def upload_to_gist(data: dict):
    gist_id    = os.environ["GIST_ID"]
    gist_token = os.environ["GIST_TOKEN"]

    payload = {
        "description": "PL 2025/26 xG data — auto-updated by GitHub Actions",
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
                body = await resp.json()
                raw_url = body["files"]["pl_xg_2025_26.json"]["raw_url"]
                print(f"\n✅ Gist updated successfully!")
                print(f"   Raw URL: {raw_url}")
            else:
                text = await resp.text()
                raise RuntimeError(f"Gist update failed ({resp.status}): {text}")


async def main():
    print(f"Fetching PL {SEASON}/{int(SEASON)+1} xG data from Understat...\n")
    async with aiohttp.ClientSession() as session:
        data = await fetch_all(session)

    print(f"\nFetched {len(data)} teams. Uploading to Gist...")
    await upload_to_gist(data)


if __name__ == "__main__":
    asyncio.run(main())
