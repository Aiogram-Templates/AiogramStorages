
![enter image description here](https://github.com/DIMFLIX-OFFICIAL/aiogram-storages/blob/main/banner.png?raw=true)
# Save your data!

**Aiogram-storages** was created to extend the standard fsm_storage options in **aiogram**.
Our library supports such databases as:


# PostgreSQL

Support for storage with the **PostgreSQL** database is due to the **asyncpg** asynchronous library, which gives a **huge data processing speed**, and, accordingly, the bot itself.

In order to use it, you need to create an instance of the **PGStorage** class, to which you need to pass the **required parameters (user, password, db_name).** You can also specify additional parameters (host, port).

Next, this instance must be passed to the **Dispatcher.**
## Example

    from aiogram_storages import PGStorage
    storage = PGStorage(username='YourUser', password='YourPassword', db_name='YourDbName')  
    dp = Dispatcher(bot, storage=storage)

## Warning

By default, **PGStorage** creates three tables in your database named: **aiogram-states**, **aiogram-data**, **aiogram-buckets**.

We **strongly recommend** that you do **not use these names as the name of the table**, otherwise there may be disagreements.



# SQLiteStorage


Support for storage with the **SQLite** database is due to the **aiosqlite** asynchronous library, which gives a **huge data processing speed**, and, accordingly, the bot itself.

In order to use it, you need to create an instance of the **SQLiteStorage** class, to which you need to pass the **required parameters (db_path).**

Next, this instance must be passed to the **Dispatcher.**
## Example

    from aiogram_storages import SQLiteStorage
    storage = SQLiteStorage(db_path='your_path')  
    dp = Dispatcher(bot, storage=storage)

## Warning

By default, **SQLiteStorage** creates three tables in your database named: **aiogram-states**, **aiogram-data**, **aiogram-buckets**.

We **strongly recommend** that you do **not use these names as the name of the table**, otherwise there may be disagreements.

