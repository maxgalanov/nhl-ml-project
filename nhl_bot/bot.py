from telebot import types, asyncio_filters
from telebot.asyncio_storage import StateMemoryStorage
import asyncio
from telebot.async_telebot import AsyncTeleBot
from bot_handlers import register_bot_commands
from config import token


bot = AsyncTeleBot(token, state_storage=StateMemoryStorage())

bot.add_custom_filter(asyncio_filters.StateFilter(bot))

register_bot_commands(bot)

async def setup_bot_commands():
    await bot.set_my_commands([
        types.BotCommand("/start", "Start command"),
        types.BotCommand("/results", "Show results of last week's games"),
        types.BotCommand("/player_stats", "Get statistics for a player"),
        types.BotCommand("/team_stats", "Get statistics for a team"),
        # types.BotCommand("/set_favorite_team", "Set favorite team"),
        types.BotCommand("/make_bet", "Guess the results of upcoming games"),
        types.BotCommand("/datalens", "Dashboard"),
        types.BotCommand("/cancel", "Cancel current operation"),
    ])

asyncio.run(setup_bot_commands())

asyncio.run(bot.polling())
