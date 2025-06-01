# app.py
import asyncio
from FinancialData import FinancialData
from utilities import Utilities

async def main():
    util = Utilities()
    data = FinancialData(util)
    await data.connect()
    schema_info = await data.get_database_info()
    print(schema_info)

    query = "SELECT * FROM journal_entries LIMIT 5;"
    result = await data.async_fetch_data_using_sqlite_query(query)
    print(result)

    await data.close()

asyncio.run(main())
