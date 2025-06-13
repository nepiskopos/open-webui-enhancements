from langgraph_sdk import get_sync_client
from pydantic import BaseModel, Field
from typing import Generator, Iterator, Optional, Union
import json
import logging
import os
import sys
import threading
import uuid


class SharedUserFilesDict:
    '''
    This is a class for creating a shared dictionary
    for storing information on the latest user-uploaded
    files. It uses a lock to ensure thread safety when
    accessing the shared dictionary.
    '''
    def __init__(self):
        self._data = dict()  # Dictionary to store user files
        self._lock = threading.Lock()  # Create a lock

    def get_user_files(self, user_id: str, chat_id: str) -> list:
        return_data = []

        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for retrieving data
            # Check if the key exists in the dictionary
            if key in self._data:
                return_data = self._data[key]

        return return_data

    def insert_user_files(self, user_id: str, chat_id: str, files: list) -> None:
        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Check if the key exists in the dictionary
            if key not in self._data:
                # User ID does not exist in the dictionary - create an entry for it
                self._data[key] = files
            else:
                # Update the existing user files with the new ones
                existing_files = self._data[key]
                # Merge new files with existing ones, avoiding duplicates
                for file in files:
                    if file not in existing_files:
                        existing_files.append(file)

    def delete_user_data(self, user_id: str, chat_id: str) -> None:
        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for writing
            # Remove user data from the dictionary
            if key in self._data:
                del self._data[key]

    def get_all_data(self) -> dict:
        with self._lock:  # Acquire the lock for writing
            return self._data


class SharedUserFilesLatestUploadDict:
    '''
    This is a class for creating a shared dictionary
    for storing the latest file upload of the users'
    files. It uses a lock to ensure thread safety when
    accessing the shared dictionary.
    '''
    def __init__(self):
        self._data = dict()
        self._lock = threading.Lock()  # Create a lock

    def get_user_latest_timestamp(self, user_id: str, chat_id: str) -> int:
        return_data = 0

        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for retrieving data
            # Check if the key exists in the dictionary
            if key in self._data:
                return_data = self._data[key]

        return return_data

    def update_user_latest_timestamp(self, user_id: str, chat_id: str, timestamp: int) -> None:
        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Check if the key exists in the dictionary
            if key not in self._data:
                # User ID does not exist in the dictionary - create an entry for it
                self._data[key] = timestamp
            elif timestamp > self._data[key]:
                # Update the latest timestamp of the user in the dictionary
                self._data[key] = timestamp


class Pipeline:
    '''
    This is a class for creating a Pipeline for censoring
    sensitive information in text originating from
    user=uploaded DOCX document files, utilizing LangGraph.
    '''
    class Valves(BaseModel):
        LITELLM_API_BASE_URL: str = Field(
            default=os.getenv(
                "LITELLM_API_BASE_URL",
                "",  # Default URL for local development
            ),
            description="URL of the LiteLLM service.",
        )
        LITELLM_API_KEY: str = Field(
            default=os.getenv(
                "LITELLM_API_KEY",
                "",  # Default API key for local development (should be replaced with a real key in production)
            ),
            description="API key for authenticating requests to the LiteLLM service.",
        )
        MODEL_ID: str = Field(default=os.getenv(
                "MODEL_ID",
                "",  # Default model ID for local development (should be replaced with a real model ID in production)
            ),
            description="Model of choice as defined in LiteLLM configuration.",
        )
        LANGGRAPH_STUDIO_URL: str = Field(default=os.getenv(
                "LANGGRAPH_STUDIO_URL",
                "",  # Default model ID for local development (should be replaced with a real model ID in production)
            ),
            description="URL of the LangGraph Studio.",
        )
        LANGSMITH_API_KEY: str = Field(default=os.getenv(
                "LANGSMITH_API_KEY",
                "",  # Default model ID for local development (should be replaced with a real model ID in production)
            ),
            description="LangSmith API key (Service Key or Personal Access Tokens).",
        )
        APP_ID: str = Field(
            default=os.getenv(
                "APP_ID",
                "PIPELINE_LANGGRAPH_DOCUMENT_GDPR_PII_IDENTIFICATION",
            ),
            description="Application name for logging.",
        )

    def __init__(self):
        '''Constructor method'''
        # Initialize OpenAI API information from environment variables through valves.
        self.valves = self.Valves()

        # Enable logging and set level to DEBUG
        logger = logging.getLogger(self.valves.APP_ID)
        logger.setLevel(logging.DEBUG)

        # Set logging parameters
        formatter = logging.Formatter(
            fmt='%(asctime)s + %(levelname)-8s + %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        handlersys = logging.StreamHandler(stream=sys.stdout)
        handlersys.setFormatter(formatter)
        logger.addHandler(handlersys)

        # Initialize user file contents dictionary
        self.user_file_contents = SharedUserFilesDict()

        # Keep a list of latest users upload timestamps
        self.user_timestamps = SharedUserFilesLatestUploadDict()

        self.langgraph_client = get_sync_client(
            url=self.valves.LANGGRAPH_STUDIO_URL,
            api_key=self.valves.LANGSMITH_API_KEY,
        )

    async def on_startup(self):
        '''This function is called when the server is started.'''
        pass

    async def on_shutdown(self):
        '''This function is called when the server is stopped.'''
        pass

    async def on_valves_updated(self):
        '''This function is called when the valves are updated.'''
        pass

    async def inlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies form data before the OpenAI API request.'''
        logging.getLogger(self.valves.APP_ID).debug(f"INLET begin:\nBody:\n{json.dumps(body, indent=2)}")

        if body.get("files", []):
            # Extract user ID
            user_id = __user__['id'] if __user__ is not None else ''

            # Extract user chat, session and file information from the request body
            chat_id = body.get('metadata', {}).get('chat_id', '')

            # Delete user-uploaded data information from the shared dictionary (if any)
            self.user_file_contents.delete_user_data(user_id, chat_id)

            # Store chat ID in the last message content to be passed to the Pipe method, in the format
            # "Original content\n\n\n\n\nChat ID\n\n\n\n\n<chat_id>\n\n\n\n\n"
            body['messages'][-1]['content'] = body['messages'][-1]['content'] + f"\n\n\n\n\nChat ID\n\n\n\n\n{chat_id}\n\n\n\n\n"


            # Retrieve the timestamp of the user's latest file uploaded
            latest_timestamp = self.user_timestamps.get_user_latest_timestamp(user_id, chat_id)

            # Check if no timestamp has been set yet
            if latest_timestamp == 0:
                # Extract the timestamp of the latest model update
                latest_timestamp = int(body.get('metadata', {}).get('model', {}).get('created', 0))

                # Update the latest timestamp of the user in the shared dictionary
                self.user_timestamps.update_user_latest_timestamp(user_id, chat_id, latest_timestamp)

            # Keep a copy of the timestamp of the user's latest file uploaded
            new_files_latest_timestamp = latest_timestamp


            file_infos = []

            # Iterate over the whole list of uploaded files
            for file_info in body.get("files", []):
                # Extract file information
                file = file_info["file"]

                # Check if file was among the latest uploaded files
                if int(file["created_at"]) > latest_timestamp:
                    file_infos.append(file_info)

                if int(file["created_at"]) > new_files_latest_timestamp:
                    # Update the latest timestamp of the uploaded files
                    new_files_latest_timestamp = int(file["created_at"])

            # Update the latest timestamp of the user in the shared dictionary
            self.user_timestamps.update_user_latest_timestamp(user_id, chat_id, new_files_latest_timestamp)


            new_files = []

            # Extract file info for all files in the body
            for file_info in body.get("files", []):
                # Add file to the list of collected new files
                if file_info["file"]["created_at"] >= latest_timestamp:
                    new_files.append(file_info)

            logging.getLogger(self.valves.APP_ID).debug(f"INLET: User-uploaded files '{new_files}'")

            # Keep new user files into the current user file contents
            self.user_file_contents.insert_user_files(user_id, chat_id, new_files)


            # Keep only new and acceptable files in the request body (removing previously processed and unacceptable files)
            body["files"] = [file_info for file_info in body["files"] if file_info in new_files]
            body["metadata"]["files"] = [file_info for file_info in body["metadata"]["files"] if file_info in new_files]

        logging.getLogger(self.valves.APP_ID).debug(f"INLET end")

        return body

    async def outlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies OpenAI response form data before returning them to the user.'''
        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET begin:\nBody:\n{json.dumps(body, indent=2)}")

        # Extract user chat information
        chat_id = body.get('chat_id', '')

        # Extract user ID
        user_id = __user__['id'] if __user__ is not None else ''

        # Delete user-uploaded data information from the shared dictionary (if any)
        self.user_file_contents.delete_user_data(user_id, chat_id)

        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET end")

        return body

    def pipe(self, user_message: str, model_id: str, messages: list[dict], body: dict) -> Union[str, Generator, Iterator]:
        '''Custom pipeline logic (like RAG).'''
        logging.getLogger(self.valves.APP_ID).debug(f"PIPE begin:\nBody:\n{json.dumps(body, indent=2)}")

        return_data = ""

        # Extract user ID from the body
        user_id = body.get('user', {}).get('id', '')

        # Extract chat ID from the messages
        chat_id = ''
        if messages[-1]['content']:
            # Extract chat ID from the last message content, assuming the chat ID is stored in the format
            # "Original content\n\n\n\n\nChat ID\n\n\n\n\n<chat_id>\n\n\n\n\n"
            # This is a workaround to extract the chat ID from the last message content as it was added in the inlet method.

            # Split the content by triple newlines to separate the parts
            content_parts = messages[-1]['content'].split("\n\n\n\n\n") if messages[-1]['content'] else [''] * 3

            # Extract the chat ID from the content parts
            chat_id = content_parts[2]

            # Restore the original content without the chat ID
            messages[-1]['content'] = content_parts[0]


        # Extract user-uploaded files (if available)
        user_files = self.user_file_contents.get_user_files(user_id, chat_id)

        if not user_files:
            logging.getLogger(self.valves.APP_ID).warning(f"PIPE: No input DOCX files were provided")
            return_data = "No compatible files were uploaded. This model only supports DOCX files with UTF-8 encoding."
        else:
            if not any(len(file_info['file']['data']['content']) for file_info in user_files):
                logging.getLogger(self.valves.APP_ID).warning(f"PIPE: All input DOCX files are empty")
                return_data = "All uploaded DOCX files are empty."
            else:
                logging.getLogger(self.valves.APP_ID).debug(f"PIPE: User input files:\n{json.dumps([file['file']['filename'] for file in user_files], indent=2)}")

                data = []

                try:
                    logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Creating thread LangGraph client thread...")

                    thread = self.langgraph_client.threads.create(
                        thread_id=str(uuid.uuid4()),
                    )


                    logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Sending user input files to the LangGraph server...")

                    graph_response = self.langgraph_client.runs.wait(
                        thread["thread_id"],
                        "agent",  # Name of assistant (defined in langgraph.json)
                        input={
                            'files': user_files,
                        },
                    )


                    logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Extracting identified PII items from the LangGraph response...")

                    data = [
                        {
                            "id": file_pii['id'],
                            "pii": file_pii['pii'],
                        } for file_pii in graph_response['final_pii_items']
                    ]
                except Exception as e:
                    logging.getLogger(self.valves.APP_ID).error(f"PIPE: Error processing files {json.dumps([file['file']['filename'] for file in user_files], indent=2)}: {e}")

                if data:
                    return_data = json.dumps(data, indent=2)
                else:
                    return_data = "Error processing the content of uploaded DOCX files."

        logging.getLogger(self.valves.APP_ID).debug(f"PIPE end")

        return return_data