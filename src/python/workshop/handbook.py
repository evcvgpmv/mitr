# pip install azure-ai-projects==1.0.0b10
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential

project_client = AIProjectClient.from_connection_string(
    credential=DefaultAzureCredential(),
    conn_str="eastus2.api.azureml.ms;61d1bfb7-ed36-4b11-abd2-ea02fb74b849;rghub;gaya101project")

agent = project_client.agents.get_agent("asst_LMRzikEakUGHWbQ365Kcs10y")

thread = project_client.agents.get_thread("thread_2vUH3LO5WemnOEmi3DkqE14N")

message = project_client.agents.create_message(
    thread_id=thread.id,
    role="user",
    content="how is the pay on holiday"
)

run = project_client.agents.create_and_process_run(
    thread_id=thread.id,
    agent_id=agent.id)
messages = project_client.agents.list_messages(thread_id=thread.id)

for text_message in messages.text_messages:
    print(text_message.as_dict())