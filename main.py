import requests
import xml.etree.cElementTree as ET
import configparser
import os
import gspread
import pandas as pd
import psycopg2
import schedule
import time
from datetime import datetime


configpath = r"config.ini"


def open_database(
    host,
    port,
    user,
    password,
    dbname
) -> psycopg2.extensions.connection:
    """
    Создает подключение к базе данных

    Args:
        host (_type_): Хост
        port (_type_): Порт
        user (_type_): Имя пользователя
        password (_type_): Пароль
        dbname (_type_): Имя базы данных

    Returns:
        psycopg2.extensions.connection: Обьект подключения к базе PostgreSQL
    """
    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
    )
    # Необходимо для использования VACUUM и избавляет от лишних commit'ов
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    return conn


def init_database(conn: psycopg2.extensions.connection) -> None:
    """
    Создаёт таблицу в базе, если её нет.
    """
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            order_id integer PRIMARY KEY,
            price_usd real,
            price_rub real,
            delivery_time date
        )
    """)
    cursor.close()


def create_config(path: str) -> None:
    """Создает файл конфигурации если его нет.
    Использует значения по умолчанию.

    Args:
        path (str): Имя файла
    """
    config = configparser.ConfigParser()

    config.set("DEFAULT", "google_key_file_path", "google_api_key.json")
    config.set(
        "DEFAULT",
        "spreadsheet_id",
        "1NEOgEBntRbH_2Mdfky4INddCwh-zxZszAAgeVjCvbq8"
    )
    config.set("DEFAULT", "currency_id", "R01235")
    config.set("DEFAULT", "order_update_sleep_time", "5")
    config.set("DEFAULT", "database_vaccum_sleep_time", "3600")
    config.set("DEFAULT", "telegram_notification_time", "12:00")

    config.add_section("POSTGRES")
    config.set("POSTGRES", "user", "postgres")
    config.set("POSTGRES", "password", "postgres")
    config.set("POSTGRES", "host", "db")
    config.set("POSTGRES", "port", "5432")
    config.set("POSTGRES", "dbname", 'kanalservis')

    config.add_section('TELEGRAM')
    config.set('TELEGRAM', 'send_notifications', 'true')
    config.set(
        'TELEGRAM',
        'bot_token',
        '1822269080:AAHGfhj0_1Pa4WuUJ6SYHRwp3YhLHrwqHYY'
    )
    config.set('TELEGRAM', 'chat_id', '1389731470')

    with open(path, "w") as config_file:
        config.write(config_file)


def read_config(path: str) -> configparser.ConfigParser:
    """Открывает и читает конфиг. Если его нет, вызывает функцию создания

    Args:
        path (str): Путь к файлу с конфигом

    Returns:
        configparser.ConfigParser: обьект взаимодействия с конфигом
    """
    if not os.path.exists(path):
        create_config(path)
    config = configparser.ConfigParser()
    config.read(path)
    return config


def get_currency_rate(currency_id: str) -> float:
    """Получает курс валюты с серверов ЦБ РФ.

    Args:
        currency_id (str): ID валюты в системе cbr.ru

    Returns:
        float: курс валюты
    """
    url = "http://www.cbr.ru/scripts/XML_daily.asp"
    with requests.Session() as session:
        res = session.get(url)
        tree = ET.fromstring(res.content)
        usd_rate = tree.find(f"Valute[@ID='{currency_id}']/Value")
        return float(usd_rate.text.replace(',', '.'))


def get_worksheet(
    spreadsheet_key: str,
    google_api_filename: str
) -> gspread.Worksheet:
    """Открывает рабочий лист в google sheets

    Args:
        spreadsheet_key (str): ID таблицы в google sheets. Берется из URL
        google_api_filename (str): Пут к json файлу с google токеном

    Returns:
        gspread.Worksheet: Обьект рабочего листа google sheets
    """
    gc = gspread.service_account(filename=google_api_filename)
    sh = gc.open_by_key(spreadsheet_key)
    worksheet = sh.get_worksheet(0)
    return worksheet


def read_records(worksheet: gspread.Worksheet) -> pd.DataFrame:
    """Читает записи в листе google sheets и переводит их в Датафрейм pandas

    Args:
        worksheet (gspread.Worksheet): Обьект листа

    Returns:
        pd.DataFrame: датафрейм со всеми записями
    """
    dataframe = pd.DataFrame(worksheet.get_all_records())
    return dataframe


def format_dataframe(
    dataframe: pd.DataFrame,
    currency_rate: float
) -> pd.DataFrame:
    """Приводит датафрейм в более читаемый вид.
     Переименовывает столбцы, убирает первый ненужный столбец,
     приводит дату в нужный тип данных,
     добавляет столбец с ценой в РУБ

    Args:
        dataframe (pd.DataFrame): Датафрейм со всеми данными
        currency_rate (float): Курс валюты к рублю

    Returns:
        pd.DataFrame: обновленный датафрейм
    """
    dataframe = dataframe.rename(
        columns={
            "№": "num",
            "заказ №": "order_id",
            "стоимость,$": "price_usd",
            "срок поставки": "delivery_time"
        }
    )
    dataframe.drop('num', inplace=True, axis=1)
    dataframe['delivery_time'] = pd.to_datetime(
        dataframe['delivery_time'],
        format="%d.%m.%Y"
    )

    dataframe['price_rub'] = dataframe.apply(
        lambda row: float(
            "{:.2f}".format(row.price_usd * currency_rate)
        ),
        axis=1
    )
    return dataframe


def update_data(
    worksheet: gspread.Worksheet,
    connection: psycopg2.extensions.connection,
    config: configparser.ConfigParser
) -> None:
    """Получает и форматирует записи,
    получает курс валют,
    вызывает функцию обновления бд

    Args:
        worksheet (gspread.Worksheet): Рабочий лист google sheets
        connection (psycopg2.extensions.connection): объект подключения к БД
        config (configparser.ConfigParser): Обьект конфига
    """
    dataframe = read_records(worksheet)
    currency_id = config['DEFAULT']['currency_id']
    currency_rate = get_currency_rate(currency_id)
    dataframe = format_dataframe(dataframe, currency_rate)
    update_records(dataframe, connection)


def update_records(
    dataframe: pd.DataFrame,
    connection: psycopg2.extensions.connection
) -> None:
    """
    Обновляет записи в базе данных. Добавляет новые,
    обновляет текущие, удаляет неактуальные.

    Args:
        dataframe (pd.DataFrame): Датафрейм со всеми данными
        connection (psycopg2.extensions.connection): объект подключения к БД
    """
    cursor = connection.cursor()
    tuples = [tuple(x) for x in dataframe.to_numpy()]
    values = [
        cursor.mogrify(
            "(%s,%s,%s,%s)",
            tup
        ).decode('utf8') for tup in tuples
    ]

    active_orders = (
        str(order_id) for order_id in dataframe['order_id'].tolist()
    )

    cursor.execute(
        """
        INSERT INTO orders
        (order_id, price_usd, delivery_time, price_rub)
        VALUES
        {}
        ON CONFLICT (order_id)
            DO UPDATE SET
                price_usd = excluded.price_usd,
                delivery_time = excluded.delivery_time,
                price_rub = excluded.price_rub
        """.format(",".join(values))
    )
    cursor.execute(
        """
        DELETE from orders
        WHERE order_id NOT IN
        ({})
        """.format(",".join(active_orders))
    )
    cursor.close()


def vacuum_database(conn: psycopg2.extensions.connection) -> None:
    """Очищает базу от мертвых кортежей

    Args:
        conn (psycopg2.extensions.connection): объект подключения к БД
    """
    cursor = conn.cursor()
    cursor.execute('VACUUM')
    cursor.close()


def notify_expired_orders(
    connection: psycopg2.extensions.connection, bot, chat_id
) -> None:
    """
    Ищет заказы с истекшими сроками поставки и
    присылает уведомления в чат telegram

    args:
        connection (psycopg2.extensions.connection): объект подключения к БД
        bot: обьект телеграм-бота
        chat_id: Id телеграм чата
    """
    cursor = connection.cursor()
    cursor.execute(
        """
            SELECT * FROM orders
            WHERE delivery_time < '{}'
        """.format(datetime.now().strftime('%Y-%m-%d'))
    )
    res = cursor.fetchall()
    for order in res:
        message_text = """
<b>Сроки поставки истекли!</b>

№ Заказа: <code>{order_id}</code>
Стоимость, $: <code>{price_usd}</code>
Стоимость, ₽: <code>{price_rub}</code>

Дэдлайн: <code>{delivery_time}</code>
        """.format(
            order_id=order[0],
            price_usd=order[1],
            price_rub=order[2],
            delivery_time=order[3],
        )

        bot.send_message(chat_id, message_text)


def main():
    # Читаем конфиг
    config = read_config(configpath)
    # Получаем рабочий лист с заказами
    worksheet = get_worksheet(
        config['DEFAULT']["spreadsheet_id"],
        config['DEFAULT']["google_key_file_path"]
    )
    # Открываем базу данных
    conn = open_database(
        config['POSTGRES']['host'],
        config['POSTGRES']['port'],
        config['POSTGRES']['user'],
        config['POSTGRES']['password'],
        config['POSTGRES']['dbname']
    )
    # Если таблицы нет - создаём
    init_database(conn)

    # Время сна между обновлениями заказов
    order_update_sleep_time = float(
        config['DEFAULT']['order_update_sleep_time']
    )
    # Время сна между очисткой базы
    database_vaccum_sleep_time = float(
        config['DEFAULT']['database_vaccum_sleep_time']
    )

    # Создаем таски для шедулера.
    schedule.every(order_update_sleep_time).seconds.do(
        update_data, worksheet, conn, config
    )
    schedule.every(database_vaccum_sleep_time).seconds.do(
        vacuum_database, conn
    )

    # Если в конфиге указано, будем отправлять уведомления в Телеграм
    send_notifications = config['TELEGRAM']['send_notifications'] == 'true'
    if send_notifications:
        import telebot
        bot_token = config['TELEGRAM']['bot_token']
        chat_id = int(config['TELEGRAM']['chat_id'])
        tg_notification_time = config['DEFAULT']['telegram_notification_time']

        bot = telebot.TeleBot(bot_token, parse_mode='HTML')
        schedule.every().day.at(tg_notification_time).do(
            notify_expired_orders, conn, bot, chat_id
        )

    while True:
        schedule.run_pending()
        time.sleep(1)


if __name__ == "__main__":
    main()
