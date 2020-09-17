import asyncio
import aiohttp
from understat import Understat
import pandas as pd
import os

async def get_data(league, year):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        t = await understat.get_teams(league, year)
        teams = [x["title"] for x in t]
        data=[]
        for team in teams:
            team_data = []
            fixtures = await understat.get_team_results(team, year)
            fixture_ids = [x["id"] for x in fixtures]
            for i in fixture_ids:
                shot_data = await understat.get_match_shots(i)
                # want shot data for the **opposition** team
                if shot_data["h"][0]["h_team"] == team:
                    wanted = "a"
                else: wanted = "h"

                prob_cs = 1
                for shot in shot_data[wanted]:
                    prob_cs *= (1 - float(shot["xG"]))
                team_data.append(round(prob_cs, 4))
            data.append([team] + team_data + [round(sum(x for x in team_data), 4)])
        
        df = pd.DataFrame(data, columns=["team"] + list(range(1, len(fixtures) + 1)) + ["total"])
        df = df.sort_values(by="total", ascending=False)

        outdir = f"team/{str(year)}-{str(year + 1)[2:]}/{league}"
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        df.to_csv(
            f"{outdir}/xCS.csv",
            index=False
            )
        print(df)


async def main():
    await get_data("EPL", 2020)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())