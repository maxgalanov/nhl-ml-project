from telebot import types
from database import DatabasePool
from telebot.asyncio_handler_backends import StatesGroup, State
import queries as q
from telebot import types
import cairosvg, requests
import io
import telebot

PLAYER_TYPES = {
    "Вратарь": "goalies_agg",
    "Полевой игрок": "skaters_agg"
}

class PlayerStates(StatesGroup):
    player = State()
    name = State()
    datalens = State()

class TeamStates(StatesGroup):
    team_name = State()

class UserStates(StatesGroup):
    favorite_team = State()

async def send_svg_as_png(bot, chat_id, svg_url):
    try:
        response = requests.get(svg_url)
        response.raise_for_status()  # Проверка на успешный ответ
        svg_data = response.content

        # Конвертация SVG в PNG
        png_data = cairosvg.svg2png(bytestring=svg_data)

        # Создание объекта BytesIO из PNG данных
        png_image = io.BytesIO(png_data)
        png_image.seek(0)  # Перемещаем указатель в начало файла

        await bot.send_photo(chat_id, png_image)
    except requests.RequestException as e:
        print(f"Ошибка загрузки SVG: {e}")
    except Exception as e:
        print(f"Ошибка при конвертации или отправке изображения: {e}")

def register_bot_commands(bot):
    db_pool = DatabasePool()

    @bot.message_handler(commands=["start"])
    async def start_message(message):
        start_text = (
            "Привет!\n"
            "Я могу показать результаты игр за неделю с помощью команды /results\n"
            "Информация по игрокам: /player_stats\n"
            "Информация по командам: /team_stats\n"
            "Выбрать любимую команду: /set_favorite_team\n"
            "Дашборды в DataLens: /datalens"
        )
        await bot.send_message(message.chat.id, start_text)

    @bot.message_handler(commands=["results"])
    async def get_results(message):
        try:
            db_pool = DatabasePool()
            results = db_pool.query_to_dataframe("""
                SELECT * 
                FROM public.games_wide_datamart 
                WHERE game_date between current_date - interval '1 week' and current_date
                    AND (home_score != 0 AND visiting_score != 0)
                ORDER BY eastern_start_time DESC, game_date
            """)
            db_pool.close_all_connections()

            response = "Результаты за последние 7 дней:\n\n"
            last_date = None

            for index, row in results.iterrows():
                if last_date is not None and last_date != row['game_date']:
                    response += "\n"  # Добавляем пустую строку между датами

                # Определение победителя и добавление эмодзи кубка
                if row['home_score'] > row['visiting_score']:
                    home_team = f"🏆 {row['home_team_name']}"
                    visiting_team = row['visiting_team_name']
                elif row['home_score'] < row['visiting_score']:
                    home_team = row['home_team_name']
                    visiting_team = f"{row['visiting_team_name']} 🏆"
                else:
                    home_team = row['home_team_name']
                    visiting_team = row['visiting_team_name']

                game_info = f"{row['game_date']}   {home_team} {row['home_score']} : {row['visiting_score']} {visiting_team}"
                response += game_info + "\n"
                last_date = row['game_date']  # Обновляем последнюю дату

            if response.endswith("\n"):
                response = response[:-1]  # Удаляем лишний перенос строки в конце, если он есть
            await bot.send_message(message.chat.id, response)
        except Exception as e:
            await bot.send_message(message.chat.id, f"Извините, произошла ошибка {e}при получении результатов.")


    @bot.message_handler(commands=["player_stats"])
    async def get_players(message):
        keys = ["Вратарь", "Полевой игрок"]
        markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
        row = [types.KeyboardButton(x) for x in keys]
        markup.add(*row)
        await bot.set_state(message.from_user.id, PlayerStates.player, message.chat.id)
        await bot.send_message(message.chat.id, "Вратарь или полевой игрок?", reply_markup=markup)

    @bot.message_handler(state=PlayerStates.player)
    async def get_name(message):
        markup = telebot.types.ReplyKeyboardRemove()

        await bot.set_state(message.from_user.id, PlayerStates.name, message.chat.id)
        await bot.send_message(message.chat.id,
                            "Введите имя, например Alex Ovechkin" if message.text == 'Полевой игрок' else "Введите имя, например Sergei Bobrovsky",
                                reply_markup=markup)

        try:
            player_type = PLAYER_TYPES[message.text]
            async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
                data["player"] = player_type
        except KeyError:
            await bot.send_message(message.chat.id, "Выбран неподдерживаемый тип игрока. Попробуйте ещё раз.")

    @bot.message_handler(state=PlayerStates.name)
    async def get_stats(message):
        player_name = message.text
        markup = telebot.types.ReplyKeyboardRemove()

        try:
            async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
                player_type = data["player"]

                if player_type == 'skaters_agg':
                    query = q.get_skaters_stats_query(player_name)
                else:
                    query = q.get_goalies_stats_query(player_name)

                db_pool = DatabasePool()
                df = db_pool.query_to_dataframe(query)

                if df.empty:
                    raise ValueError("Игрок не найден.")
                
            player_info = df.iloc[0]

            await bot.send_photo(message.chat.id, player_info['headshot'], reply_markup=markup)

            response = "Дата рождения: " + str(player_info["birth_date"]) + "\n"
            response += "Возраст: " + str(int(player_info["years_old"])) + "\n"
            response += "Страна: " + str(player_info["birth_country"]) + "\n"
            response += "Город: " + str(player_info["birth_city"]) + "\n"
            response += "Команда: " + str(player_info["team_full_name"]) + " (" + str(player_info["team_business_id"]) + ")" + "\n"
            response += "Игр сыграно: " + str(int(player_info["game_cnt"])) + "\n"
            if player_type == 'skaters_agg':
                response += "Очков: " + str(int(player_info["points"])) + "\n"
                response += "Голов: " + str(int(player_info["goals"])) + "\n"
                response += "Показатель полезности: " + str(int(player_info["plus_minus"])) + "\n"
            else:
                response += "Процент отраженных бросков: " + str(round(player_info["shots_against_pctg"], 2)) + "\n"

            await bot.send_message(message.chat.id, response, reply_markup=markup)

            if player_type == 'skaters_agg':
                await bot.send_message(
                    message.chat.id,
                    f"""Более подробную информацию можете посмотреть в нашем [дашборде по игрокам](https://datalens.yandex/xqnhz02g6x6ml?tab=lD&player_full_name_s={player_name.split(' ')[0]}%20{player_name.split(' ')[1]})""",
                    parse_mode="MarkdownV2",
                    )
            elif player_type == "goalies_agg":
                await bot.send_message(
                    message.chat.id,
                    f"""Более подробную информацию можете посмотреть в нашем [дашборде по игрокам](https://datalens.yandex/xqnhz02g6x6ml?tab=24G&player_full_name_g={player_name.split(' ')[0]}%20{player_name.split(' ')[1]})""",
                    parse_mode="MarkdownV2",
                )
            await bot.delete_state(message.from_user.id, message.chat.id)

        except Exception as e:
            await bot.send_message(message.chat.id, str(e), reply_markup=markup)


    @bot.message_handler(commands=["team_stats"])
    async def get_team_stats(message):
        markup = types.ReplyKeyboardRemove()
        await bot.send_message(message.chat.id, "Введите название команды или трикод, например 'Washington Capitals' или 'WSH'", reply_markup=markup)
        await bot.set_state(message.from_user.id, TeamStates.team_name, message.chat.id)


    @bot.message_handler(state=TeamStates.team_name)
    async def display_team_stats(message):
        team_name = message.text
        stats_query = q.get_team_stats_query(team_name)

        try:
            db_pool = DatabasePool()
            stats_df = db_pool.query_to_dataframe(stats_query)

            if stats_df.empty:
                raise ValueError("Команда не найдена.")

            team_info = stats_df.iloc[0]

            logo_query = q.get_team_logo_query(team_info['team_name'])
            logo_df = db_pool.query_to_dataframe(logo_query)

            db_pool.close_all_connections()
            
            logo_url = logo_df['team_logo'].iloc[0]

            # Отправка логотипа команды
            await send_svg_as_png(bot, message.chat.id, logo_url)
            
            response = f"{team_info['team_name']}\n"
            response += f"Конференция: {team_info['conference_name']}\n"
            response += f"Дивизион: {team_info['division_name']}\n"
            response += f"Сыграно матчей: {team_info['games_played']}\nМесто в лиге: {team_info['league_sequence']}\nОчки: {team_info['points']}\n"
            response += f"Побед: {team_info['wins']} ({100 * team_info['win_pctg']:.2f}%)\nЗабито голов: {team_info['goal_for']}, Пропущено голов: {team_info['goal_against']}"

            await bot.send_message(message.chat.id, response, reply_markup=types.ReplyKeyboardRemove())
            await bot.send_message(
                    message.chat.id,
                    f"""Более подробную информацию можете посмотреть в нашем [дашборде по командам](https://datalens.yandex/xqnhz02g6x6ml?tab=jAE&team_name_field_id={team_info['team_name'].split(' ')[0]}%20{team_info['team_name'].split(' ')[1]})""",
                    parse_mode="MarkdownV2",
                )
            await bot.delete_state(message.from_user.id, message.chat.id)

        except Exception as e:
            await bot.send_message(message.chat.id, str(e), reply_markup=types.ReplyKeyboardRemove())


    @bot.message_handler(state="*", commands=["cancel"])
    async def cancel(message):
        await bot.send_message(message.chat.id, "Отмена")
        await bot.delete_state(message.from_user.id, message.chat.id)

    @bot.message_handler(commands=["datalens"])
    async def get_datalens(message):
        keys = ["Турнирная таблица", "Лидерборд", "Статистика команды", 
                "Игроки на карте", "Форварды и защитники", "Вратари", 
                "Статистика полевого игрока", "Статистика вратаря"]
        markup = telebot.types.ReplyKeyboardMarkup(resize_keyboard=True)
        row = [telebot.types.KeyboardButton(option) for option in keys]
        markup.add(*row)

        await bot.set_state(message.from_user.id, PlayerStates.datalens, message.chat.id)
        await bot.send_message(message.chat.id, "Выберите вкладку дашборда", reply_markup=markup)

    @bot.message_handler(state=PlayerStates.datalens)
    async def get_datalens(message):
        datalens_type = {
            "Игроки на карте": "7pV",
            "Форварды и защитники": "LD",
            "Вратари": "Re",
            "Статистика полевого игрока": "lD",
            "Статистика вратаря": "24G",
            "Турнирная таблица": "LMJ",
            "Лидерборд": "AGK",
            "Статистика команды": "jAE",
        }

        try:
            dashboard_code = datalens_type[message.text]
            dashboard_url = f"https://datalens.yandex/xqnhz02g6x6ml?tab={dashboard_code}"
            markup = telebot.types.ReplyKeyboardRemove()

            await bot.send_message(
                message.chat.id,
                f'[Дашборд в DataLens "{message.text}"]({dashboard_url})',
                parse_mode="MarkdownV2",
                reply_markup=markup
            )
            await bot.delete_state(message.from_user.id, message.chat.id)
        except KeyError:
            await bot.send_message(message.chat.id, "Выбрана неизвестная вкладка. Пожалуйста, попробуйте ещё раз.")

    @bot.message_handler(commands=["set_favorite_team"])
    async def set_favorite_team(message):
        db_pool = DatabasePool()
        teams_df = db_pool.query_to_dataframe(q.get_teams_query())
        teams_list = list(teams_df['team_name'])

        markup = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)

        for i in range(0, len(teams_list), 4):
            row = teams_list[i:i+4]
            markup.row(*(types.KeyboardButton(name) for name in row))
        markup.add(types.KeyboardButton("Сбросить команду"))
        
        await bot.send_message(message.chat.id, "Выберите вашу любимую команду:", reply_markup=markup)
        await bot.set_state(message.from_user.id, UserStates.favorite_team, message.chat.id)


    @bot.message_handler(state=UserStates.favorite_team)
    async def save_favorite_team(message):
        team_name = message.text
        user_id = message.from_user.id
        
        if team_name == "Сбросить команду":
            db_pool = DatabasePool()
            db_pool.execute_query(f"DELETE FROM public.favorite_teams WHERE user_id = {user_id}")
            await bot.send_message(message.chat.id, "Команда сброшена.", reply_markup=types.ReplyKeyboardRemove())
        else:
            db_pool = DatabasePool()
            try:
                db_pool.execute_query(f"""
                    INSERT INTO public.favorite_teams (user_id, team_id, selected_at) VALUES ({user_id}, '{team_name}', NOW())
                    ON CONFLICT (user_id) DO UPDATE SET team_id = EXCLUDED.team_id, selected_at = NOW();
                """)
                await bot.send_message(message.chat.id, f"Ваша любимая команда теперь {team_name}.", reply_markup=types.ReplyKeyboardRemove())
            except Exception as e:
                await bot.send_message(message.chat.id, f"Произошла ошибка {e}.", reply_markup=types.ReplyKeyboardRemove())

        await bot.delete_state(message.from_user.id, message.chat.id)