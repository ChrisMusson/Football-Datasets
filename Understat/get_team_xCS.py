import asyncio
import aiohttp
from understat import Understat
import pandas as pd
import os

async def get_data(league, year):
    async with aiohttp.ClientSession() as session:
        understat = Understat(session)
        t = await understat.get_teams(league, year)
        max_fixtures = max(len(x["history"]) for x in t)
        teams = [x["title"] for x in t]
        data=[]
        for team in teams:
            team_data = ["-"] * max_fixtures
            fixtures = await understat.get_team_results(team, year)
            fixture_ids = [x["id"] for x in fixtures]
            for i, fix in enumerate(fixture_ids):
                try:
                    shot_data = await understat.get_match_shots(fix)
                    # want shot data for the **opposition** team
                    prob_cs = 1
                    try:
                        if shot_data["h"][0]["h_team"] == team:
                            wanted = "a"
                        else:
                            wanted = "h"
                        for shot in shot_data[wanted]:
                            prob_cs *= (1 - float(shot["xG"]))
                    except IndexError:  # occurs when the home team had 0 shots
                        try:
                            if shot_data["a"][0]["h_team"] == team:
                                wanted = "a"
                            else:
                                wanted = "h"
                            for shot in shot_data[wanted]:
                                prob_cs *= (1 - float(shot["xG"]))
                        except IndexError:  # occurs when the away team also had 0 shots
                            pass

                    team_data[i] = round(prob_cs, 4)

                except UnboundLocalError:  # occurs when no match data is present e.g. for abandoned matches
                    pass

            data.append([team] + team_data + [round(sum(x for x in team_data if x != "-"), 4)])
        
        df = pd.DataFrame(data, columns=["team"] + list(range(1, max_fixtures + 1)) + ["total"])
        df = df.sort_values(by="total", ascending=False)

        outdir = f"team/{str(year)}-{str(year + 1)[2:]}/{league}"
        if not os.path.exists(outdir):
            os.mkdir(outdir)
        df.to_csv(
            f"{outdir}/xCS.csv",
            index=False
            )


async def main():
    leagues = ["EPL", "La Liga", "Bundesliga", "Serie A", "Ligue 1", "RFPL"]
    for l in leagues:
        await get_data(l, 2020)

if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())