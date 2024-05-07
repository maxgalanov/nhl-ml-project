import os

token = os.getenv("NHL_BOT_TOKEN")

postgres = {
    "host": os.getenv("HSE_DB_HOST"),
    "port": os.getenv("HSE_DB_PORT"),
    "dbname": os.getenv("HSE_DB_NAME"),
    "user": os.getenv("HSE_DB_USER"),
    "passwd": os.getenv("HSE_DB_PASSWORD"),
}