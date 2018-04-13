import asyncio

import aioredis

from config import config

from sanic import Sanic
from sanic.response import text

import json

redis = None
app = Sanic()


# Helpers
async def get_redis_client():
    global redis

    if redis is not None:
        return redis

    redis = await aioredis.create_redis(
        (config["redis"]["host"], config["redis"]["port"]),
        loop=asyncio.get_event_loop()
    )

    return redis


# Endpoints
@app.route("/<channel_id>", methods=["GET"])
async def verification(request, channel_id):
    if request.args.get("hub.mode") == "denied":
        return text("K")

    redis_client = await get_redis_client()

    channel_id = request.args.get("hub.topic").split("=")[1]
    redis_key = f"{config['redis']['prefix']}:CHANNEL_NAME:{channel_id}"

    channel_name = await redis_client.get(redis_key)
    redis_key = f"{config['redis']['prefix']}:SUBSCRIPTION:{channel_name.decode('utf-8')}"

    if request.args.get("hub.mode") == "subscribe":
        expire = int(request.args.get("hub.lease_seconds"))
        await redis_client.setex(redis_key, expire, 1)
    elif request.args.get("hub.mode") == "unsubscribe":
        await redis_client.delete(redis_key)

    return text(request.args.get("hub.challenge"))


@app.route("/<channel_id>", methods=["POST"])
async def notification(request, channel_id):
    data = request.json["data"]
    redis_client = await get_redis_client()

    if len(data):
        message = {
            "topic": "CHANNEL:ONLINE",
            "payload": {
                "channel_id": channel_id,
                "game_id": data[0]["game_id"],
                "type": data[0]["type"],
                "title": data[0]["title"]
            }
        }
    else:
        message = {
            "topic": "CHANNEL:OFFLINE",
            "payload": {
                "channel_id": channel_id
            }
        }

    await redis_client.lpush(f"{config['redis']['prefix']}:MESSAGES", json.dumps(message))

    return text("THX BB")


app.run(host="0.0.0.0", port=51415, debug=False)
