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
        types.BotCommand("/start", "Старт"),
        types.BotCommand("/results", "Показать результаты игр за последние 7 дней"),
        types.BotCommand("/player_stats", "Статистика игроков"),
        types.BotCommand("/team_stats", "Статистика команд"),
        # types.BotCommand("/set_favorite_team", "Выбрать любимую команду"),
        types.BotCommand("/make_bet", "Сделать прогноз на исход ближайших матчей"),
        types.BotCommand("/show_preds", "Посмотреть прогноз ML модели на ближайшие матчи"),
        types.BotCommand("/datalens", "Дашборды в DataLens"),
        types.BotCommand("/cancel", "Сбросить введенные данные"),
    ])

asyncio.run(setup_bot_commands())

asyncio.run(bot.polling())
