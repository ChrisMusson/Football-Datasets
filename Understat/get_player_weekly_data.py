import asyncio
import aiohttp
from understat import Understat
import pandas as pd
import os
import html


async def get_data(league, year):
    int_stats = ["goals", "shots", "time", "assists", "key_passes", "npg"]
    float_stats = ["xG", "xA", "npxG", "xGChain", "xGBuildup"]
    stats = int_stats + float_stats

    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        t = await understat.get_teams(league, year)
        max_fixtures = max(len(x["history"]) for x in t)
        teams = [x["title"] for x in t]
        data = []

        for team in teams:
            fixtures = await understat.get_team_results(team, year)
            fixture_ids = [x["id"] for x in fixtures]
            players = await understat.get_team_players(team, year)
            for player in players:
                y = await understat.get_player_matches(player["id"], season=str(year))
                p_matches = [x["id"] for x in y]

                statData = [["-"] * max_fixtures for _ in range(len(stats))]
                for i, ID in enumerate(p_matches):
                    try:
                        for j, stat in enumerate(stats):
                            if stat in int_stats:
                                statData[j][fixture_ids.index(ID)] = int(y[i][stat])
                            else:
                                statData[j][fixture_ids.index(ID)] = round(float(y[i][stat]), 4)
                    except:
                        pass

                data.append(
                    [
                        [player["id"], html.unescape(player["player_name"]), player["team_title"]] +
                        statData[i] +
                        [round(sum(x for x in statData[i] if x != "-"), 4)]
                        for i in range(len(statData))
                    ]
                )

        outdir = f"player/{str(year)}-{str(year + 1)[2:]}/{league}"
        if not os.path.exists(outdir):
            os.mkdir(outdir)

        for i, stat in enumerate(stats):
            df = pd.DataFrame(
                [data[x][i] for x in range(len(data))],
                columns=["id", "name", "team"] +
                list(range(1, max_fixtures + 1)) +
                ["total"]
            )

            df = df.sort_values(by="total", ascending=False)

            df.to_csv(
                f"player/{str(year)}-{str(year + 1)[2:]}/{league}/{stat}.csv",
                index=False
            )


async def main():
    leagues = ["EPL", "La Liga", "Bundesliga", "Serie A", "Ligue 1", "RFPL"]
    # leagues = ["EPL", "La Liga", "Bundesliga", "Serie A"]
    return await asyncio.gather(*[get_data(l, 2019) for l in leagues])

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
