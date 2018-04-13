import asyncio

from autobahn.asyncio.wamp import ApplicationSession, ApplicationRunner
from autobahn.wamp.types import RegisterOptions, PublishOptions
from autobahn.wamp import auth

from config import config

import aioredis
import discord
import json
import requests


class DiscordBotComponent:
    @classmethod
    def run(cls):
        print(f"Starting {cls.__name__}...")

        url = f"ws://{config['crossbar']['host']}:{config['crossbar']['port']}"

        runner = ApplicationRunner(url=url, realm=config["crossbar"]["realm"])
        runner.run(DiscordBotWAMPComponent)


class DiscordBotWAMPComponent(ApplicationSession):
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
        self.messages = dict()
        
        self.discord = discord.Client()

        @self.discord.event
        async def on_ready():
            bot_channel = None

            for server in self.discord.servers:
                if server.name == config["discord"]["server"]:
                    for channel in server.channels:
                        if channel.name == config["discord"]["channel"]:
                            bot_channel = channel

            async def on_online(payload):
                try:
                    channel = await self.fetch_channel(payload['channel_id'])
                    game = await self.fetch_game(payload['game_id'])

                    escaped_display_name = channel['display_name'].replace('_', '\\_')

                    twitch_url = f"https://www.twitch.tv/{channel['login']}"

                    message = f"@everyone {escaped_display_name} has gone live! {twitch_url}"
                    embed = discord.Embed(title=twitch_url, colour=discord.Colour(0x4b367c))

                    embed.set_thumbnail(url=channel['profile_image_url'])
                    embed.set_author(name=f"{channel['display_name']} is now streaming!", url=twitch_url)
                    embed.set_footer(text=config["discord"]["server"])

                    embed.add_field(name="Category", value=game['name'], inline=False)
                    embed.add_field(name="Title", value=payload["title"], inline=False)

                    discord_message = await self.discord.send_message(bot_channel, message, embed=embed)

                    self.messages[payload["channel_id"]] = discord_message
                except Exception:
                    pass

            async def on_offline(payload):
                try:
                    message = self.messages.get(payload['channel_id'])

                    if message:
                        await self.discord.delete_message(message)
                except Exception:
                    pass

            await self.subscribe(on_online, "CHANNEL:ONLINE")
            await self.subscribe(on_offline, "CHANNEL:OFFLINE")

        await self.discord.login(config["credentials"]["discord"]["bot_user_token"])
        await self.discord.connect()

    async def fetch_channel(self, channel_id):
        redis_key = f"{config['redis']['prefix']}:CHANNEL:{channel_id}"
        channel_data = await self.redis_client.get(redis_key)

        if channel_data is None:
            url = f"https://api.twitch.tv/helix/users?id={channel_id}"

            response = requests.get(
                url,
                headers={"Client-ID": config["credentials"]["twitch"]["client_id"]}
            )

            channel_data = response.json()["data"][0]
            await self.redis_client.setex(redis_key, 1209600, json.dumps(channel_data))
        else:
            channel_data = json.loads(channel_data.decode("utf-8"))

        return channel_data

    async def fetch_game(self, game_id):
        redis_key = f"{config['redis']['prefix']}:GAME:{game_id}"
        game_data = await self.redis_client.get(redis_key)

        if game_data is None:
            url = f"https://api.twitch.tv/helix/games?id={game_id}"

            response = requests.get(
                url,
                headers={"Client-ID": config["credentials"]["twitch"]["client_id"]}
            )

            game_data = response.json()["data"][0]
            await self.redis_client.set(redis_key, json.dumps(game_data))
        else:
            game_data = json.loads(game_data.decode("utf-8"))

        return game_data

    async def _initialize_redis_client(self):
        return await aioredis.create_redis(
            (config["redis"]["host"], config["redis"]["port"]),
            loop=asyncio.get_event_loop()
        )

if __name__ == "__main__":
    DiscordBotComponent.run()
