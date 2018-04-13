import asyncio

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.types import RegisterOptions, PublishOptions
from autobahn.wamp import auth

from config import config

import aioredis
import json


class PublisherComponent:
    @classmethod
    def run(cls):
        print(f"Starting {cls.__name__}...")

        url = f"ws://{config['crossbar']['host']}:{config['crossbar']['port']}"

        runner = ApplicationRunner(url=url, realm=config["crossbar"]["realm"])
        runner.run(PublisherWAMPComponent)


class PublisherWAMPComponent(ApplicationSession):
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

        while True:
            redis_key, message = await self.redis_client.brpop(f"{config['redis']['prefix']}:MESSAGES")
            message = json.loads(message.decode("utf-8"))

            topic = message.pop("topic")
            self.publish(topic, message["payload"])
            print(topic, message)

    async def _initialize_redis_client(self):
        return await aioredis.create_redis(
            (config["redis"]["host"], config["redis"]["port"]),
            loop=asyncio.get_event_loop()
        )

if __name__ == "__main__":
    PublisherComponent.run()
