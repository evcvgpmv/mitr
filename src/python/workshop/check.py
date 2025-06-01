import os
from dotenv import load_dotenv
from azure.identity import DefaultAzureCredential
from azure.ai.projects import AIProjectClient

# Load environment variables from .env file
load_dotenv()

# Get the connection string
#project_connection_string = os.getenv("PROJECT_CONNECTION_STRING")
project_connection_string = "resource-id=/subscriptions/61d1bfb7-ed36-4b11-abd2-ea02fb74b849/resourceGroups/rsg1/providers/Microsoft.CognitiveServices/accounts/newresource1gaya567/projects/firstProject"
project_connection_string = "eastus2.api.azureml.ms;61d1bfb7-ed36-4b11-abd2-ea02fb74b849;rghub;gaya101project"
# Create a credential (make sure you're logged in via Azure CLI or environment variables)
credential = DefaultAzureCredential()

# Connect to your Azure AI Foundry project
project_client = AIProjectClient.from_connection_string(
    credential=credential,
    conn_str=project_connection_string,
)

# Fetch basic project metadata
project = project_client.get_project()
print(f"Connected to project: {project.name}")
