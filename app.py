import traceback
import json
import os
import asyncio
import aiohttp
import aiohttp_jinja2
import jinja2
from aiohttp import web
import aiofiles
from pigz_python import PigzFile
from datetime import datetime
routes = web.RouteTableDef()
from settings import HUB, PORT

async def fetch_remote_data(app):
    # This function will update the app["aircrafts"] list and store the last 30 copies of the aircrafts.json file
    # It will also notify the function store_data() if the last_aircrafts_ts is more than 5 seconds old
    last_aircrafts_ts = 0
    try:
        async with aiohttp.ClientSession(timeout=1) as session:
            while True:
                # aircrafts.json
                ips = [HUB]
                for ip in ips:
                    aircrafts = []
                    try:
                        async with session.get(f"http://{ip}/aircraft.json") as resp:
                                aircrafts = await resp.json()

                                # Store the last 30 copies of the aircrafts.json file
                                # If it's the same as the last one, don't store it
                                if len(app["aircrafts"]) > 0 and app["aircrafts"][-1] == aircrafts:
                                    print("Same aircrafts.json, not storing")
                                    continue
                                if len(app["aircrafts"]) > 30:
                                    app["aircrafts"].pop(0)
                                    print("aircrafts.json list is too long, removing the oldest one")
                                app["aircrafts"].append(aircrafts)
                                print("aircrafts.json stored, len=", len(app["aircrafts"]))

                                await store_data(app)
                    except Exception as e:
                        print(f"Error fetching aircrafts.json from {ip}: {e}")
                        traceback.print_exc()
                                      
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("Background task cancelled")

async def store_data(app):
    # This function will look at app["aircrafts"] and pick,
    # a candidate to be stored.
    # The candidate should be at least 4 seconds old, and not older than 6 seconds compared to the last stored copy (app["last_stored_at"])
    # If a candidate is found, it will be stored in a file and the app["last_stored_at"] will be updated
    try:
        while True:
            # Find the candidate
            candidate = None
            # Check from last to first
            for aircraft in app["aircrafts"][::-1]:
                now = float(aircraft["now"])
                tiers = {
                    'gold': [5.5, 4.5],
                    'silver': [7, 5.5],
                    'bronze': [10, 7],
                    'wood': [15, 10],
                    'plastic': [20, 15],
                    'paper': [30, 20],
                    'stone': [60, 30],
                }
                # Ensure if it does not exist yet, it will be stored
                if not app["last_stored_at"]:
                    candidate = aircraft
                    print('last_stored_at does not exist yet')
                    break
                for tier, (max_age, min_age) in tiers.items():
                    if now - app["last_stored_at"] > min_age and now - app["last_stored_at"] < max_age:
                        candidate = aircraft
                        print(f'tier={tier} now={now} last_stored_at={app["last_stored_at"]}')
                        break
            if not candidate:
                print('No candidate found')
                return
            # Store the candidate in /out/YYYY/MM/DD/HH/MM/adsblol-YYYY-MM-DD-HH-MM-SS.json (pigz)
            candidate_datetime = datetime.fromtimestamp(candidate["now"])
            candidate_datetime_str = candidate_datetime.strftime("%Y/%m/%d/%H/%M/adsblol-%Y-%m-%d-%H-%M-%S.json")
            candidate_path = f"/out/{candidate_datetime_str}"
            # make directory and subdirectories
            os.makedirs(os.path.dirname(candidate_path), exist_ok=True)
            async with aiofiles.open(candidate_path, mode='wb') as f:
                await f.write(json.dumps(candidate).encode('utf-8'))
            # Update app["last_stored_at"]
            app["last_stored_at"] = candidate["now"]
            print(f'stored={candidate_path}')

            await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("Background task cancelled")



async def background_task_1(app):
    app["fetch_remote_data"] = asyncio.create_task(fetch_remote_data(app))

    yield

    app["fetch_remote_data"].cancel()
    await app["fetch_remote_data"]

# aiohttp server
app = web.Application()
app.add_routes(routes)
app["aircrafts"] = []
app["last_stored_at"] = 0
# add background tasks
# Make sure they run parallel
app.cleanup_ctx.append(background_task_1)

if __name__ == "__main__":
    aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('/app/templates'))
    web.run_app(app, host="0.0.0.0", port=PORT)
