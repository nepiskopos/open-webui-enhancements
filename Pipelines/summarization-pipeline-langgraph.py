from langgraph_sdk import get_sync_client
from pydantic import BaseModel, Field
from typing import Any, Generator, Iterator, Optional, Union
import httpx
import json
import logging
import os
import sys
import threading
import uuid


class SharedUserFilesLatestUploadDict:
    '''
    This is a class for creating a shared dictionary for
    storing the latest uploaded files for each user-chat
    combination. It uses a lock to ensure thread safety
    when accessing the shared dictionary.
    '''
    def __init__(self):
        self._data = dict()
        self._lock = threading.Lock()  # Create a lock

    def get_user_latest_timestamp(self, user_id: str, chat_id: str) -> int:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for retrieving data
            # If key exists in the dictionary, return its value, otherwise return 0
            return_data = self._data.get(key, 0)

        return return_data

    def update_user_latest_timestamp(self, user_id: str, chat_id: str, timestamp: int) -> None:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # If key exists in the dictionary, compare update its value to the timestamp
            if timestamp > self._data.setdefault(key, 0):
                # Update latest timestamp
                self._data[key] = timestamp

class SharedUserFilesDict:
    '''
    This is a class for creating a shared dictionary
    for storing the latest file upload timestamp for
    each user-chat combination. It uses a lock to ensure
    thread safety when accessing the shared dictionary.
    '''
    def __init__(self):
        self._data = dict()
        self._lock = threading.Lock()  # Create a lock

    def add_user_file_info(self, user_id: str, chat_id: str, file_info: dict[str, Any]) -> None:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Update file name
            self._data.setdefault(key, []).append(file_info)

    def add_user_file_infos(self, user_id: str, chat_id: str, file_infos: list[dict[str, Any]]) -> None:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Update file name
            self._data.setdefault(key, []).extend(file_infos)

    def get_user_files_info(self, user_id: str, chat_id: str) -> list[dict[str, Any]]:
        return_data = None

        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for retrieving data
            # If key exists in the dictionary, return its value, otherwise return 0
            return_data = self._data.get(key, [])

        return return_data

    def clear_user_files_info(self, user_id: str, chat_id: str) -> None:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for clearing data
            # Clear user files info
            if key in self._data:
                del self._data[key]

class Pipeline:
    '''
    This is a class for creating a Pipeline for creating summaries of
    user-uploaded documents, utilizing an external LangGraph server.
    '''
    class Valves(BaseModel):
        LANGGRAPH_SERVICE_URL: str = Field(
            default=os.getenv(
                "LANGGRAPH_SERVICE_URL",
                ""
            ),
            description="URL of the LangGraph service.",
        )
        LANGSMITH_API_KEY: str = Field(
            default=os.getenv(
                "LANGSMITH_API_KEY",
                ""
            ),
            description="API key for LangSmith.",
        )
        APP_ID: str = Field(
            default=os.getenv(
                "APP_ID",
                "PIPELINE_DOCUMENT_SUMMARIZATION_LANGGRAPH",
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

        # Keep the latest file upload timestamp for each user-chat combination
        self.user_timestamps = SharedUserFilesLatestUploadDict()

        # Keep the latest uploaded files for each user-chat combination
        self.user_files = SharedUserFilesDict()

        # Ensure the URL has http:// prefix
        service_url = self.valves.LANGGRAPH_SERVICE_URL
        if not service_url.startswith(("http://", "https://")):
            service_url = f"http://{service_url}"
            logger.info(f"Added http:// prefix to LANGGRAPH_SERVICE_URL: {service_url}")

        self.client = get_sync_client(url=service_url, api_key=self.valves.LANGSMITH_API_KEY)

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
        logging.getLogger(self.valves.APP_ID).debug(f"INLET begin")
        # logging.getLogger(self.valves.APP_ID).debug(f"INLET begin:\nBody: {json.dumps(body, indent=2)}")

        # Check if the body contains files
        if body.get("files", []):
            # Extract user ID and chat information
            user_id = __user__['id']
            chat_id = body.get('metadata', {}).get('chat_id', '')

            # Extract model creation timestamp from the request body
            model_timestamp = body.get('metadata', {}).get('model', {}).get('created', 0)

            # Update the user-chat timestamp in the shared dictionary
            self.user_timestamps.update_user_latest_timestamp(user_id, chat_id, model_timestamp)

            # Retrieve latest user-chat timestamp from the shared dictionary
            latest_timestamp = self.user_timestamps.get_user_latest_timestamp(user_id, chat_id)

            for file_info in body["files"][::-1]:
                # Extract file creation timestamp from the request body
                file_timestamp = file_info.get('file', {}).get('updated_at', 0)

                # If the file was uploaded after the latest timestamp, update the user files
                if file_timestamp > latest_timestamp:
                    # Create an OIFile instance and insert it into the shared dictionary
                    self.user_files.add_user_file_info(
                        user_id=user_id,
                        chat_id=chat_id,
                        file_info=file_info
                    )

                    # Update the user-chat timestamp in the shared dictionary
                    self.user_timestamps.update_user_latest_timestamp(user_id, chat_id, file_timestamp)

        logging.getLogger(self.valves.APP_ID).debug(f"INLET end")

        return body

    async def outlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies OpenAI response form data before returning them to the user.'''
        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET begin")
        # logging.getLogger(self.valves.APP_ID).debug(f"OUTLET begin:\nBody: {json.dumps(body, indent=2)}")

        user_id = __user__['id']
        chat_id = body.get('chat_id', '')

        self.user_files.clear_user_files_info(user_id, chat_id)

        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET end")

        return body

    def pipe(
        self, user_message: str, model_id: str, messages: list[dict], body: dict
    ) -> Union[str, Generator, Iterator]:
        '''Custom pipeline logic (like RAG).'''
        logging.getLogger(self.valves.APP_ID).debug(f"PIPE begin")
        # logging.getLogger(self.valves.APP_ID).debug(f"PIPE begin: Body:\n{json.dumps(body, indent=2)}")
        # logging.getLogger(self.valves.APP_ID).debug(f"PIPE begin: Messages:\n{json.dumps(messages, indent=2)}")

        # Prepare return data
        return_data = ''

        if body.get("metadata", {}).get("task", None) is None:
            # Extract user ID from the body
            user_id = body.get('user', {}).get('id', '')

            # Define a list to store files to process
            files_infos_to_process = []

            for msg in messages[::-1]:
                # logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Message: {msg}")

                if msg["role"] == "user":
                    try:
                        msg_content_json = json.loads(msg["content"])

                        if msg_content_json:
                            # Extract chat ID from the message content
                            chat_id = msg_content_json.get("chat_id", "")

                            files_infos_to_process = self.user_files.get_user_files_info(user_id, chat_id)

                            # logging.getLogger(self.valves.APP_ID).debug(f"PIPE: User input files to process for user ID: {user_id} and chat ID: {chat_id}: {files_infos_to_process}")
                        else:
                            logging.getLogger(self.valves.APP_ID).warning(f"PIPE: No user input files found for user ID: {user_id}")

                        break
                    except json.JSONDecodeError:
                        logging.getLogger(self.valves.APP_ID).warning(f"PIPE: Could not decode user message content as JSON: {msg['content']}")
                        continue

            if not files_infos_to_process:
                return_data = "ERROR: No compatible files were uploaded. This model only supports uploading documents and raw text files."
                logging.getLogger(self.valves.APP_ID).warning(f"PIPE: No input files were provided")
            else:
                if not any(user_file['file']['meta']['size'] for user_file in files_infos_to_process):
                    return_data = "ERROR: All uploaded documents are empty."
                    logging.getLogger(self.valves.APP_ID).warning(f"PIPE: All user-uploaded documents are empty")
                else:
                    try:
                        # logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Creating LangGraph thread for processing user-uploaded documents")

                        # Create thread with unique ID
                        thread = self.client.threads.create(thread_id=str(uuid.uuid4()))

                        # logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Thread created with ID: {thread['thread_id']}")

                        # Run agent with document
                        res = self.client.runs.wait(
                            thread['thread_id'],  # Threadless run
                            "agent",  # Name of assistant (defined in langgraph.json)
                            input={
                                "files": files_infos_to_process,
                            },
                        )

                        if res and 'result' in res:
                            result = res['result']
                            return_data = json.dumps([{
                                'id': f['id'],
                                'filename': f['name'],
                                'summary': f['summary']
                                } for f in result.values() if f]
                            , indent=2)
                            # logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Result: {result}")
                        elif res and "__error__" in res:
                            # Check if there's an error in the response
                            error_type = res["__error__"].get("error", "Unknown")
                            error_message = res["__error__"].get("message", "No error message provided")
                            error_msg = f"PIPE: LangGraph service error: {error_type} - {error_message}"

                            if "ModelNotFound" in error_message:
                                return_data = "ERROR: The document processing model could not be found on the server. Please contact support."
                            else:
                                return_data = "ERROR: The document processing service encountered an error. Please try again later."

                            logging.getLogger(self.valves.APP_ID).error(error_msg)
                        else:
                            return_data = "ERROR: Error processing the contents of uploaded documents."
                            logging.getLogger(self.valves.APP_ID).error(f"PIPE: Missing 'result' in response: {res}")

                    except KeyError as e:
                        logging.getLogger(self.valves.APP_ID).error(f"PIPE: Missing key in response: {str(e)}")
                        return_data = "ERROR: Unexpected response format from document processing service."

                    except TimeoutError:
                        logging.getLogger(self.valves.APP_ID).error("PIPE: Request timed out while processing document")
                        return_data = "ERROR: The request timed out while processing your document. Please try again later."

                    except httpx.ConnectError as e:
                        logging.getLogger(self.valves.APP_ID).error(f"PIPE: Connection error to LangGraph service: {str(e)}")
                        return "ERROR: Cannot connect to document processing service. Please check if the service is running."

                    except ConnectionError as e:
                        logging.getLogger(self.valves.APP_ID).error(f"PIPE: Connection error when communicating with language processing service: {str(e)}")
                        return_data = "ERROR: Unable to connect to document processing service. Please try again later."

                    except Exception as e:
                        logging.getLogger(self.valves.APP_ID).error(f"PIPE: Unexpected error processing document: {str(e)}", exc_info=True)
                        return_data = "ERROR: An unexpected error occurred while processing your document."

        logging.getLogger(self.valves.APP_ID).debug("PIPE end")
        # logging.getLogger(self.valves.APP_ID).debug(f"PIPE end: Return data:\n{return_data}")

        return return_data