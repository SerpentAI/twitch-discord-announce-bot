import asyncio

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.types import RegisterOptions
from autobahn.wamp import auth

from config import config

import aioredis
import requests
import json


class SubscriptionManagerComponent:
    @classmethod
    def run(cls):
        print(f"Starting {cls.__name__}...")

        url = f"ws://{config['crossbar']['host']}:{config['crossbar']['port']}"

        runner = ApplicationRunner(url=url, realm=config["crossbar"]["realm"])
        runner.run(SubscriptionManagerWAMPComponent)


class SubscriptionManagerWAMPComponent(ApplicationSession):
    def __init__(self, c=None):
        super().__init__(c)

    def onConnect(self):
        self.join(config["crossbar"]["realm"], ["wampcra"], config["crossbar"]["auth"]["username"])

    def onDisconnect(self):
        print("Disconnected from Crossbar!")

    def onChallenge(self, challenge):
        secret = config["crossbar"]["auth"]["password"]
        signature = auth.compute_wcs(secret.encode('utf8'), challenge.extra['challenge'].encode('utf8'))

        return signature.decode('ascii')

    async def onJoin(self, details):
        self.redis_client = await self._initialize_redis_client()

        async def subscribe(channel):
            await self.register_webhook(channel, action="subscribe")

        async def unsubscribe(channel):
            await self.register_webhook(channel, action="unsubscribe")

        await self.register(subscribe, f"{config['crossbar']['realm']}.subscribe", options=RegisterOptions(invoke="roundrobin"))
        await self.register(unsubscribe, f"{config['crossbar']['realm']}.unsubscribe", options=RegisterOptions(invoke="roundrobin"))

        channels = config["channels"]

        redis_key = f"{config['redis']['prefix']}:SUBSCRIPTION:*"
        channel_keys = await self.redis_client.keys(redis_key)

        for channel_key in channel_keys:
            channel = channel_key.decode("utf-8").split(":")[-1]

            if channel not in channels:
                await self.register_webhook(channel, action="unsubscribe")

        while True:
            for channel in channels:
                redis_key = f"{config['redis']['prefix']}:SUBSCRIPTION:{channel}"

                channel_has_subscription = await self.redis_client.exists(redis_key)
                channel_has_subscription = channel_has_subscription == 1

                if not channel_has_subscription:
                    await self.register_webhook(channel, action="subscribe")
                else:
                    continue

            await asyncio.sleep(300)

    async def register_webhook(self, channel, action="subscribe"):
        if action not in ["subscribe", "unsubscribe"]:
            return None

        channel_id = await self.fetch_channel_id(channel)

        url = "https://api.twitch.tv/helix/webhooks/hub"
        data = {
            "hub.callback": f"{config['webhook_callback_url']}/{channel_id}",
            "hub.mode": action,
            "hub.topic": f"https://api.twitch.tv/helix/streams?user_id={channel_id}",
            "hub.lease_seconds": 864000,
            "hub.secret": config["webhook_secret"]
        }

        requests.post(
            url,
            data=json.dumps(data),
            headers={
                "Client-ID": config["credentials"]["twitch"]["client_id"],
                "Content-Type": "application/json"
            }
        )

    async def fetch_channel_id(self, channel):
        redis_key = f"{config['redis']['prefix']}:CHANNEL_ID:{channel}"
        channel_id = await self.redis_client.get(redis_key)

        if channel_id is None:
            url = f"https://api.twitch.tv/helix/users?login={channel}"
            response = requests.get(
                url,
                headers={"Client-ID": config["credentials"]["twitch"]["client_id"]}
            )

            channel_id = response.json()["data"][0]["id"]
            await self.redis_client.set(redis_key, channel_id)

            redis_key = f"{config['redis']['prefix']}:CHANNEL_NAME:{channel_id}"
            await self.redis_client.set(redis_key, channel)

            return channel_id
        else:
            return channel_id.decode("utf-8")

    async def _initialize_redis_client(self):
        return await aioredis.create_redis(
            (config["redis"]["host"], config["redis"]["port"]),
            loop=asyncio.get_event_loop()
        )

if __name__ == "__main__":
    SubscriptionManagerComponent.run()
