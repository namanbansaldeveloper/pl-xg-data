async def fetch_player_stats(understat, team_xg_data):
    """
    Refactored: Fetches PL players and uses 'Last N' logic for match logs.
    This bypasses date-matching issues across different team schedules.
    """
    print("\nFetching player season stats...")
    players = await understat.get_league_players("epl", SEASON)
    print(f"  {len(players)} players found")

    player_rows = []

    for i, p in enumerate(players):
        pid   = p["id"]
        pname = p["player_name"]
        team  = p["team_title"]

        if team not in PL_TEAMS:
            continue

        try:
            # Fetch all logs for the player for the specific season
            logs = await understat.get_player_matches(pid, {"season": SEASON})
        except Exception as e:
            print(f"  ⚠ Could not fetch logs for {pname}: {e}")
            continue

        # 1. Filter for Premier League only (Handles 'EPL', 'epl', or 'Premier League')
        epl_logs = [
            m for m in logs 
            if m.get("league", "").lower() in ("epl", "premier league")
        ]

        # 2. Sort by date descending (Newest matches first)
        epl_logs.sort(key=lambda x: x["date"], reverse=True)

        # 3. Define slices: Last 1 (Current GW) and Last 6 (Recent Form)
        l1 = epl_logs[:1]
        l6 = epl_logs[:6]

        def sum_stat(log_list, stat):
            return round(sum(float(m.get(stat, 0) or 0) for m in log_list), 2)

        player_rows.append({
            "id":   pid,
            "name": pname,
            "team": team,
            "season": {
                "xG":         round(float(p.get("xG", 0) or 0), 2),
                "npxG":       round(float(p.get("npxG", 0) or 0), 2),
                "xA":         round(float(p.get("xA", 0) or 0), 2),
                "shots":      int(p.get("shots", 0) or 0),
                "key_passes": int(p.get("key_passes", 0) or 0),
                "goals":      int(p.get("goals", 0) or 0),
                "assists":    int(p.get("assists", 0) or 0),
                "games":      int(p.get("games", 0) or 0),
            },
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

        if (i + 1) % 50 == 0:
            print(f"  Processed {i+1} players...")

    print(f"  Done — {len(player_rows)} PL players processed with form data.")
    return player_rows
