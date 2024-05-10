from telebot import types
from database import DatabasePool
from datetime import datetime
from telebot.asyncio_handler_backends import StatesGroup, State
from urllib.parse import quote
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

class BetStates(StatesGroup):
    select_game = State()
    select_winner = State()
    enter_score = State()

async def send_svg_as_png(bot, chat_id, svg_url):
    try:
        response = requests.get(svg_url)
        response.raise_for_status()
        svg_data = response.content

        # Конвертация SVG в PNG
        png_data = cairosvg.svg2png(bytestring=svg_data)

        png_image = io.BytesIO(png_data)
        png_image.seek(0)

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
            "Вот, что я умею:\n"
            "Показать результаты игр за последние 7 дней: /results\n"
            "Статистика игроков: /player_stats\n"
            "Статистика команд: /team_stats\n"
            # "Выбрать любимую команду: /set_favorite_team\n"
            "Сделать прогноз на исход ближайших матчей: /make_bet\n"
            "Посмотреть прогноз ML модели на ближайшие матчи: /show_preds\n"
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
                    response += "\n\n"

                # Определение победителя и добавление эмодзи кубка
                if row['home_score'] > row['visiting_score']:
                    home_team = f"🏆 <b>{row['home_team_name']}</b>"
                    visiting_team = row['visiting_team_name']
                elif row['home_score'] < row['visiting_score']:
                    home_team = row['home_team_name']
                    visiting_team = f"<b>{row['visiting_team_name']}</b> 🏆"
                else:
                    home_team = row['home_team_name']
                    visiting_team = row['visiting_team_name']

                game_info = f"{row['game_date']}   {home_team} {row['home_score']} : {row['visiting_score']} {visiting_team}"
                response += game_info + "\n\n"
                last_date = row['game_date']  # Обновляем последнюю дату

            if response.endswith("\n"):
                response = response[:-1]
            await bot.send_message(message.chat.id, response, parse_mode='HTML')
        except Exception as e:
            await bot.send_message(message.chat.id, f"Извините, произошла ошибка {e} при получении результатов.")


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
                            "Введите имя, например, Alex Ovechkin" if message.text == 'Полевой игрок' else "Введите имя, например, Sergei Bobrovsky",
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

            # Отправка фото игрока
            await bot.send_photo(message.chat.id, player_info['headshot'], reply_markup=markup)

            # Формируем сообщение с основной информацией об игроке
            response = (
                f"Дата рождения: {player_info['birth_date']}\n"
                f"Возраст: {int(player_info['years_old'])}\n"
                f"Страна: {player_info['birth_country']}\n"
                f"Город: {player_info['birth_city']}\n"
                f"Команда: {player_info['team_full_name']} ({player_info['team_business_id']})\n"
                f"Игр сыграно: {int(player_info['game_cnt'])}\n"
            )

            if player_type == 'skaters_agg':
                response += (
                    f"Очков: {int(player_info['points'])}\n"
                    f"Голов: {int(player_info['goals'])}\n"
                    f"Показатель полезности: {int(player_info['plus_minus'])}\n"
                )
            else:
                response += f"Процент отраженных бросков: {round(player_info['shots_against_pctg'], 2)}\n"

            await bot.send_message(message.chat.id, response, reply_markup=markup)

            player_name_encoded = quote(player_name)

            if player_type == 'skaters_agg':
                dashboard_url = f"https://datalens.yandex/xqnhz02g6x6ml?tab=lD&player_full_name_s={player_name_encoded}"
            else:
                dashboard_url = f"https://datalens.yandex/xqnhz02g6x6ml?tab=24G&player_full_name_g={player_name_encoded}"

            await bot.send_message(
                message.chat.id,
                f"Более подробную информацию можете посмотреть в нашем [дашборде по игрокам]({dashboard_url})",
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

            team_name_encoded = quote(team_info['team_name'])

            dashboard_url = f"https://datalens.yandex/xqnhz02g6x6ml?tab=jAE&team_name_field_id={team_name_encoded}"

            await bot.send_message(
                message.chat.id,
                f"Более подробную информацию можете посмотреть в нашем [дашборде по командам]({dashboard_url})",
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


    ######################################################
    ##################### Users Bets #####################
    ######################################################

    @bot.message_handler(commands=["make_bet"])
    async def make_bet(message):
        user_id = message.from_user.id
        games_df = db_pool.query_to_dataframe(q.get_upcoming_games_query(user_id))

        if games_df.empty:
            await bot.send_message(message.chat.id, "Нет матчей для прогнозов.")
            return

        markup = types.InlineKeyboardMarkup()
        for index, row in games_df.iterrows():
            moscow_time = datetime.strptime(str(row['moscow_time']), '%Y-%m-%d %H:%M:%S%z').strftime('%m-%d %H:%M')
            bet_indicator = "✅" if row['bet_placed'] else ""
            button_text = f"{moscow_time} {row['home_team_name']} vs {row['visiting_team_name']} {bet_indicator}"
            markup.add(types.InlineKeyboardButton(text=button_text, callback_data=f"game_{row['game_source_id']}"))

        await bot.send_message(message.chat.id, "Выберите матч для прогноза:", reply_markup=markup)
        await bot.set_state(message.from_user.id, BetStates.select_game, message.chat.id)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("game_"))
    async def select_winner(call):
        game_id = int(call.data.split("_")[1])
        game_details = db_pool.query_to_dataframe(q.get_game_details_query(game_id))

        if game_details.empty:
            await bot.send_message(call.message.chat.id, "Информация о матче не найдена.")
            return

        game = game_details.iloc[0]
        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton(game['home_team_name'], callback_data=f"winner_{game_id}_home"),
            types.InlineKeyboardButton(game['visiting_team_name'], callback_data=f"winner_{game_id}_visiting")
        )

        await bot.send_message(call.message.chat.id, "Выберите победителя:", reply_markup=markup)
        await bot.set_state(call.from_user.id, BetStates.select_winner, call.message.chat.id)

    @bot.callback_query_handler(func=lambda call: call.data.startswith("winner_"))
    async def enter_score(call):
        game_id, winner_team = call.data.split("_")[1], call.data.split("_")[2]
        winner = True if winner_team == 'home' else False

        async with bot.retrieve_data(call.from_user.id, call.message.chat.id) as data:
            data['game_id'] = game_id
            data['home_team_winner'] = winner

        markup = types.InlineKeyboardMarkup()
        markup.add(
            types.InlineKeyboardButton("Да", callback_data=f"score_{game_id}"),
            types.InlineKeyboardButton("Нет", callback_data=f"noscore_{game_id}")
        )

        await bot.send_message(call.message.chat.id, "Хотите предсказать точный счет?", reply_markup=markup)


    @bot.callback_query_handler(func=lambda call: call.data.startswith("score_") or call.data.startswith("noscore_"))
    async def save_bet(call):
        game_id = int(call.data.split("_")[1])
        if call.data.startswith("noscore_"):
            async with bot.retrieve_data(call.from_user.id, call.message.chat.id) as data:
                db_pool.execute_query(q.save_user_bet(call.from_user.id, data['game_id'], data['home_team_winner']))
            await bot.send_message(call.message.chat.id, "Ваш прогноз сохранен!")
            await bot.delete_state(call.from_user.id, call.message.chat.id)
        elif call.data.startswith("score_"):
            await bot.send_message(call.message.chat.id, "Введите счет в формате 'home:away'")
            await bot.set_state(call.from_user.id, BetStates.enter_score, call.message.chat.id)


    @bot.message_handler(state=BetStates.enter_score)
    async def process_score(message):
        try:
            home_score, visiting_score = map(int, message.text.split(":"))
        except ValueError:
            await bot.send_message(message.chat.id, "Некорректный формат. Попробуйте еще раз.")
            return

        async with bot.retrieve_data(message.from_user.id, message.chat.id) as data:
            home_team_winner = data['home_team_winner']

            # Проверка соответствия счета с выбранным победителем
            if home_team_winner and home_score <= visiting_score:
                await bot.send_message(message.chat.id, "Предполагаемый счет неверен, так как ожидается победа домашней команды.")
                return
            elif not home_team_winner and home_score >= visiting_score:
                await bot.send_message(message.chat.id, "Предполагаемый счет неверен, так как ожидается победа гостевой команды.")
                return

            # Сохранение корректного счета в БД
            db_pool.execute_query(q.save_user_bet(message.from_user.id, 
                                                data['game_id'], 
                                                home_team_winner, 
                                                home_score, 
                                                visiting_score))

        await bot.send_message(message.chat.id, "Ваш прогноз со счетом сохранен!")
        await bot.delete_state(message.from_user.id, message.chat.id)


    ######################################################
    ################### ML predictions ###################
    ######################################################

    @bot.message_handler(commands=["show_preds"])
    async def show_preds(message):
        games_df = db_pool.query_to_dataframe(q.get_upcoming_preds_query())

        if games_df.empty:
            await bot.send_message(message.chat.id, "Нет предстоящих матчей с прогнозами.")
            return

        response = "Вот предсказания на ближайшие 2 дня:\n\n"

        for index, row in games_df.iterrows():
            moscow_time = datetime.strptime(str(row['moscow_time']), '%Y-%m-%d %H:%M:%S%z').strftime('%m-%d %H:%M')

            winner = row['home_team_name'] if row['home_team_win'] else row['visiting_team_name']
            if row['home_team_win']:
                response += f"{moscow_time} 🏆 <b>{row['home_team_name']}</b> vs {row['visiting_team_name']}\n\n"
            else:
                response += f"{moscow_time} {row['home_team_name']} vs <b>{row['visiting_team_name']}</b> 🏆\n\n"
        response += "* Сортировка по убыванию уверенности модели."
        await bot.send_message(message.chat.id, response, parse_mode='HTML')
