import os
import time
import logging
import asyncio
from dotenv import load_dotenv
from azure.identity.aio import DefaultAzureCredential
from azure.ai.projects.aio import AIProjectClient
from sales_data import SalesData
from terminal_colors import TerminalColors as tc
from utilities import Utilities
from azure.ai.projects.models import (
    Agent,
    AgentThread,
    AsyncFunctionTool,
    AsyncToolSet,
    FileSearchTool,
    CodeInterpreterTool,
)

async def main():
    project_client = AIProjectClient.from_connection_string(
        credential=DefaultAzureCredential(),
        conn_str="eastus2.api.azureml.ms;61d1bfb7-ed36-4b11-abd2-ea02fb74b849;rghub;gaya101project")

    agent = await project_client.agents.get_agent("asst_T6SL8Shio6uWTDbW5rMqOIV6")

    thread = await project_client.agents.get_thread("thread_oxNFjhqRqvjUBEo7Rlsvex0C")

    toolset = AsyncToolSet()
    utilities = Utilities()
    sales_data = SalesData(utilities)
    await sales_data.connect()
    db_schema =  await sales_data.get_database_info()
    INSTRUCTIONS_FILE = "instructions/code_interpreter.txt"
    instructions = utilities.load_instructions(INSTRUCTIONS_FILE)
    instructions = instructions.replace("{database_schema_string}", db_schema)

    functions = AsyncFunctionTool({sales_data.async_fetch_sales_data_using_sqlite_query})
    toolset.add(functions)

    updated_agent =  project_client.agents.update_agent(
        agent_id=agent.id,
        instructions=agent.instructions,  # keep existing instructions or update as needed
        toolset=toolset,                  # pass the new toolset
        temperature=agent.temperature,
        model=agent.model
    )

    message = await project_client.agents.create_message(
    thread_id=thread.id,
    role="user",
    content="What are the sales by region?"
)

    run = project_client.agents.create_and_process_run(
        thread_id=thread.id,
        agent_id=agent.id)
    
    time.sleep(10)  # Increase delay if needed

    # Step 8: Retrieve and Display Messages in Correct Order
    try:
        messages = await  project_client.agents.list_messages(thread_id=thread.id)
        for text_message in messages.text_messages:
            print(text_message.as_dict())

        if hasattr(messages, "data") and messages.data:
            # Sort messages by 'created_at' timestamp in ascending order
            sorted_messages = sorted(messages.data, key=lambda x: x.created_at)

            for msg in sorted_messages:
                if msg.content and isinstance(msg.content, list):
                    for content_item in msg.content:
                        if content_item["type"] == "text":
                            print(f"🤖 {content_item['text']['value']}")
    except Exception as e:
        print(f"❌ Error retrieving messages: {e}")

if __name__ == "__main__":
    asyncio.run(main())