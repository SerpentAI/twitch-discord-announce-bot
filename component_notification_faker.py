import asyncio

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.types import RegisterOptions, PublishOptions
from autobahn.wamp import auth

import aioredis

from config import config

import random
import uuid


class NotificationFakerComponent:
    @classmethod
    def run(cls):
        print(f"Starting {cls.__name__}...")

        url = f"ws://{config['crossbar']['host']}:{config['crossbar']['port']}"

        runner = ApplicationRunner(url=url, realm=config["crossbar"]["realm"])
        runner.run(NotificationFakerWAMPComponent)


class NotificationFakerWAMPComponent(ApplicationSession):
    def __init__(self, c=None):
        super().__init__(c)

        self.channels = dict()

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

        for channel in config["channels"]:
            self.channels[channel] = False

        while True:
            await asyncio.sleep(random.uniform(5.0, 30.0))

            channel = random.choice(config["channels"])

            online = not self.channels[channel]
            self.channels[channel] = online

            redis_key = f"{config['redis']['prefix']}:CHANNEL_ID:{channel}"

            channel_id = await self.redis_client.get(redis_key)
            channel_id = channel_id.decode("utf-8")

            if online:
                topic = "CHANNEL:ONLINE"
                payload = {
                    "channel_id": channel_id,
                    "game_id": random.choice(["494717", "488191", "417752"]),
                    "type": random.choice(["live", "vodcast"]),
                    "title": f"{config['discord']['server']} - {uuid.uuid4()}"
                }
            else:
                topic = "CHANNEL:OFFLINE"
                payload = {
                    "channel_id": channel_id
                }

            message = {
                "topic": topic,
                "payload": payload
            }

            topic = message.pop("topic")
            self.publish(topic, message["payload"])
            print(topic, message)

    async def _initialize_redis_client(self):
        return await aioredis.create_redis(
            (config["redis"]["host"], config["redis"]["port"]),
            loop=asyncio.get_event_loop()
        )

if __name__ == "__main__":
    NotificationFakerComponent.run()
