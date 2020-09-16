import asyncio
import aiohttp
from understat import Understat
import pandas as pd


async def get_data(league, year):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        t = await understat.get_teams(league, year)
        teams = [x["title"] for x in t]

        data = []
        for team in teams:
            fixtures = await understat.get_team_results(team, year)
            fixture_ids = [x["id"] for x in fixtures]
            players = await understat.get_team_players(team, year)
            for player in players:
                y = await understat.get_player_matches(player["id"], season=str(year))
                p_matches = [x["id"] for x in y]

                xgData = ["-"] * len(fixture_ids)
                for i, ID in enumerate(p_matches):
                    try:
                        xgData[fixture_ids.index(ID)] = float(y[i]["xG"])
                    except:
                        pass

                data.append(
                    [player["id"], player["player_name"], player["team_title"]] +
                    xgData +
                    [sum(float(x) for x in xgData if x != "-")]
                )

        df = pd.DataFrame(
            data,
            columns=["id", "name", "team"] +
            list(range(1, len(fixtures) + 1)) +
            ["total"]
        )

        df = df.sort_values(by="total", ascending=False)

        df.to_csv(
            f"{league}_{str(year)[2:]}-{int(str(year)[2:]) + 1}.csv",
            index=False
        )


async def main():
    await get_data("EPL", 2020)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())
