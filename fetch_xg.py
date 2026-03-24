async def fetch_player_stats(understat, team_xg_data):
    print("\nStarting Player Data Fetch...")
    players = await understat.get_league_players("epl", SEASON)
    player_rows = []

    for i, p in enumerate(players):
        pid, pname, team = p["id"], p["player_name"], p["team_title"]
        if team not in PL_TEAMS: continue

        try:
            # TRY THIS: Remove the season filter from the API call and filter manually
            logs = await understat.get_player_matches(pid) 
        except Exception as e:
            continue

        # --- DEBUG SECTION: RUNS ONLY FOR THE FIRST 2 PLAYERS ---
        if i < 2:
            print(f"\nDEBUG for {pname}:")
            if not logs:
                print("  ❌ API returned ZERO logs for this player.")
            else:
                print(f"  ✅ API returned {len(logs)} total logs.")
                print(f"  Sample Log Keys: {list(logs[0].keys())}")
                print(f"  Sample Season Value: '{logs[0].get('season')}'")
                print(f"  Sample League Value: '{logs[0].get('league')}'")
        # -------------------------------------------------------

        # NEW FILTER: More aggressive matching
        # We look for "2025" in the season and "epl" in the league (case-insensitive)
        epl_logs = []
        for m in logs:
            m_season = str(m.get("season", ""))
            m_league = str(m.get("league", "")).lower()
            
            if "2025" in m_season and ("epl" in m_league or "premier" in m_league):
                epl_logs.append(m)

        epl_logs.sort(key=lambda x: x["date"], reverse=True)
        l1, l6 = epl_logs[:1], epl_logs[:6]

        # Check if we actually found anything
        if i < 2:
            print(f"  Filtered EPL Logs for 2025: {len(epl_logs)}")

        def sum_stat(log_list, stat):
            return round(sum(float(m.get(stat, 0) or 0) for m in log_list), 2)

        player_rows.append({
            "id": pid, "name": pname, "team": team,
            "season": {
                "xG": round(float(p.get("xG", 0) or 0), 2),
                "npxG": round(float(p.get("npxG", 0) or 0), 2),
                "xA": round(float(p.get("xA", 0) or 0), 2),
                "games": int(p.get("games", 0) or 0),
            },
            "last6": {
                "xG": sum_stat(l6, "xG"),
                "xA": sum_stat(l6, "xA"),
                "games": len(l6),
            },
            "lastGW": {
                "xG": sum_stat(l1, "xG"),
                "xA": sum_stat(l1, "xA"),
                "games": len(l1),
            }
        })
    return player_rows
