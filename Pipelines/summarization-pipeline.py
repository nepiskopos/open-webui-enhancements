from pydantic import BaseModel, Field
from pydantic_core import core_schema
from requests.adapters import HTTPAdapter
from typing import Any, Generator, Iterator, Optional, Union
from urllib3.util.retry import Retry
import html
import json
import logging
import os
import re
import requests
import sys
import threading


class OIFile:
    '''
    This is a class for representing a user-uploaded document object.
    It stores the document ID, the document name, the document mime type
    and the document content. It also provides methods to access these
    attributes, and to add and access two additional and optional attributes,
    which are the document summary and the document upload timestamp.

    Attributes:
    - id (str): Unique identifier for the file.
    - name (str): Name of the file.
    - type (str): MIME type of the file.
    - content (str): Normalized content of the file.
    - summary (str, optional): Summary of the file content.
    - timestamp (int, optional): Timestamp of when the file was uploaded.
    '''
    def __init__(self, id: str, name: str, type: str, content: str):
        self.id = id
        self.name = name
        self.type = type
        self.content = self._build_content(content)
        self.summary = None
        self.timestamp = None

    def get_id(self) -> str:
        return self.id

    def get_name(self) -> str:
        return self.name

    def get_type(self) -> str:
        return self.type

    def get_content(self) -> str:
        return self.content

    def get_size(self) -> int:
        return len(self.content)

    def get_summary(self) -> str:
        return self.summary or ''

    def get_timestamp(self) -> int:
        return self.timestamp or 0

    def set_summary(self, summary: str) -> None:
        self.summary = summary

    def set_timestamp(self, timestamp: int) -> None:
        self.timestamp = timestamp

    def _build_content(self, text_content: str) -> str:
        text = ''

        if text_content:
            text = text_content

            # Step 1: Remove HTML comments
            text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

            # Step 2: Remove HTML comments (duplicate step in original code)
            text = re.sub(r'<!--.*?-->', '', text, flags=re.DOTALL)

            # Step 3: Remove HTML tags
            text = re.sub(r'<[^>]+>', '', text)

            # Step 4: Decode HTML entities like &nbsp;
            text = html.unescape(text)

            # Step 5: Fix spacing issues
            # Normalize multiple spaces
            text = re.sub(r' +', ' ', text)

            # Normalize newlines (no more than two consecutive)
            text = re.sub(r'\n{3,}', '\n\n', text)

            # Step 6: Fix specific layout issues from the document
            # Fix broken lines that should be together (like "Αριθμός Γ.Ε.ΜΗ .: 180526838000")
            text = re.sub(r'([a-zA-Zα-ωΑ-Ω])\.\s+:', r'\1.:', text)

            # Step 7: Remove extra spaces before punctuation
            text = re.sub(r' ([.,:])', r'\1', text)

            # Clean up trailing whitespace on each line
            text = '\n'.join(line.rstrip() for line in text.splitlines())

            # Clean up whitespaces at the beginning and ending of each string
            text.strip()

        return text

    def __repr__(self) -> str:
        return f"File(id={self.id}, name={self.name}, type={self.type}, size={len(self.content)} bytes)"

    @classmethod
    def __get_pydantic_core_schema__(
        cls, _source_type: Any, _handler: Any
    ) -> core_schema.CoreSchema:
        """Tell Pydantic how to serialize/deserialize OIFile objects."""
        return core_schema.union_schema([
            # Handle OIFile instance
            core_schema.is_instance_schema(OIFile),
            # Convert dict to OIFile
            core_schema.chain_schema([
                core_schema.dict_schema(),
                core_schema.no_info_plain_validator_function(
                    lambda d: OIFile(
                        id=d.get("id"),
                        name=d.get("name"),
                        type=d.get("type"),
                        content=d.get("content"),
                    )
                ),
            ]),
        ])

    def to_dict(self) -> dict:
        """Convert OIFile instance to a dictionary."""
        return {
            "id": self.id,
            "name": self.name,
            "type": self.type,
            "content": self.content,
            "summary": self.summary
        }


class SharedUserFilesDict:
    '''
    This is a class for creating a shared dictionary for
    storing the latest uploaded files for each user-chat
    combination. It uses a lock to ensure thread safety
    when accessing the shared dictionary.
    '''
    def __init__(self):
        self._data = dict()
        self._lock = threading.Lock()  # Create a lock

    def add_user_file_info(self, user_id: str, chat_id: str, file_info: OIFile) -> None:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Update file name
            self._data.setdefault(key, []).append(file_info)

    def add_user_file_infos(self, user_id: str, chat_id: str, file_infos: list[OIFile]) -> None:
        # Construct index key combining user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Update file name
            self._data.setdefault(key, []).extend(file_infos)

    def get_user_files_info(self, user_id: str, chat_id: str) -> list[OIFile]:
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


class SharedUserFilesLatestUploadDict:
    '''
    This is a class for creating a shared dictionary
    for storing the latest file upload timestamp for
    each user-chat combination. It uses a lock to ensure
    thread safety when accessing the shared dictionary.
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


class Pipeline:
    '''
    This is a class for creating a Pipeline for creating
    summaries of user-uploaded documents, utilizing direct
    API calls to a LLM model hosted on an external service.
    '''
    class Valves(BaseModel):
        LITELLM_API_BASE_URL: str = Field(
            default=os.getenv(
                "LITELLM_API_BASE_URL",
                "",
            ),
            description="URL of the LiteLLM service.",
        )
        LITELLM_API_KEY: str = Field(
            default=os.getenv(
                "LITELLM_API_KEY",
                "",
            ),
            description="API key for authenticating requests to the LiteLLM service.",
        )
        MODEL_ID: str = Field(default=os.getenv(
                "MODEL_ID",
                "",
            ),
            description="Model of choice as defined in LiteLLM configuration.",
        )
        APP_ID: str = Field(
            default=os.getenv(
                "APP_ID",
                "PIPELINE_DOCUMENT_SUMMARIZATION",
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
        self.user_files = SharedUserFilesDict()

        # Keep a list of latest users upload timestamps
        self.user_timestamps = SharedUserFilesLatestUploadDict()

        # Initialize LLM endpoint API URL
        if self.valves.LITELLM_API_BASE_URL.startswith(("http://", "https://")):
            self.service_url = self.valves.LITELLM_API_BASE_URL
        else:
            self.service_url = f"http://{self.valves.LITELLM_API_BASE_URL}"
            logger.info(f"Added http:// prefix to LITELLM_API_BASE_URL {self.valves.LITELLM_API_BASE_URL}")

        # Initialize HTTP headers for the LLM endpoint API requests
        self.http_headers = {
            "Authorization": f"Bearer {self.valves.LITELLM_API_KEY}",
            "Content-Type": "application/json",
        }

        self.system_prompt = '''
            You are a document and literature analysis assistant specialized in identifying important information in text documents.
            Your response should be consise and focused on the most relevant information, and should not include any personal opinions or interpretations.
            Your response should be written in a neutral tone, without any bias or subjective language.
        '''

        self.user_prompt_template = '''
            ### Instruction:
            Your task is to analyze the following text and generate a summarization of it, which contains the most important information contained in it.

            ### Input:
            {text}

            ### Response:
            Please provide a concise summary of the text above, focusing on the most relevant information.
            Your summary should be clear and easy to understand, highlighting key points and important details.
            Your summary should have the form of a single paragraph, with no more than 200 words.
        '''

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

            for file_info in self._extract_body_files(body["files"][::-1]):
                # Extract file creation timestamp from the request body
                file_timestamp = file_info.get_timestamp()

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

    def pipe(self, user_message: str, model_id: str, messages: list[dict], body: dict) -> Union[str, Generator, Iterator]:
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
                if not any(file.get_size() for file in files_infos_to_process):
                    return_data = "ERROR: All uploaded documents are empty."
                    logging.getLogger(self.valves.APP_ID).warning(f"PIPE: All user-uploaded documents are empty")
                else:
                    logging.getLogger(self.valves.APP_ID).debug(f"PIPE: User input files to process:\n{files_infos_to_process}")

                    data = []

                    for file in files_infos_to_process:
                        try:
                            logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Getting file summary using the LLM model...")

                            summary = self._get_summary(file.get_content())

                            data.append({
                                "id": file.get_id(),
                                "filename": file.get_name(),
                                "summary": summary,
                            })
                        except Exception as e:
                            logging.getLogger(self.valves.APP_ID).error(f"PIPE: Error processing file {file.get_name()}: {e}")

                    if data:
                        return_data = json.dumps(data, indent=2)
                    else:
                        return_data = "Error processing the content of uploaded DOCX files."

        logging.getLogger(self.valves.APP_ID).debug(f"PIPE end")

        return return_data

    def _extract_body_files(self, data: list[dict] | dict) -> list[OIFile]:
        '''
        This function extracts file information from the provided data.

        It processes the data to create a list of OIFile instances, which represent user-uploaded files.
        The function checks if the input data is a single dictionary or a list of dictionaries, which each
        dictionary represents a single file.
        - If it's a dictionary, it extracts the "files" key.
        - If it's a list, it uses the list directly. It then iterates over the entries, checking if the file
        is a document file with content. If it is, it creates an OIFile instance for each file and appends
        it to the list of user-chat files.

        Input parameters:
        * data: A dictionary or list containing file information.

        Returns:
        * A list of OIFile instances representing the user-uploaded files.
        '''
        files = []

        if isinstance(data, dict):
            # If inlet_body is a dictionary (full inlet body), extract the "files" key
            files_entries = [data]
        elif isinstance(data, list):
            # If inlet_body is a list (extracted "files" from inlet body), use it directly
            files_entries = data

        for entry in files_entries:
            file_info = entry.get("file", {})
            if file_info.get('data', {}).get('content', ''):
                # Create an OIFile instance and append it to the list
                file = OIFile(file_info['id'], file_info['filename'], file_info['meta']['content_type'] , file_info['data']['content'])
                file.set_timestamp(file_info.get('updated_at', 0))

                files.append(file)

        return files

    # Change from async function to regular function
    def _get_summary(self, text: str, max_retries: int=5, wait_interval: int=10) -> str:
        '''
        This function sends a request to the OpenAI API to get a summarization of the provided text.
        If the text is empty, it returns a message indicating that no content was provided.
        If an error occurs during the request, it returns an error message.

        Input parameters:
        * text: The text to be summarized.

        Returns:
        * The summary of the text.
        '''
        summary = ""

        if not text.strip():
            summary = "No content provided for summarization."
        else:
            # Check API key before making request
            if not self.valves.LITELLM_API_KEY:
                logging.getLogger(self.valves.APP_ID).error(f"_get_summary: Invalid API key configuration")
                summary = "Error: API key not properly configured. Please set a valid LITELLM_API_KEY environment variable."
            else:
                if self._check_litellm_status():
                    logging.getLogger(self.valves.APP_ID).debug(f"_get_summary: LiteLLM service is running")

                    # Create LLM summarization payload
                    summarization_payload = {
                        "model": self.valves.MODEL_ID,
                        "messages": [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": self.user_prompt_template.format(text=text)}
                        ],
                        "temperature": 0.1,
                        "stream": False,
                    }

                    url = f"{self.service_url.rstrip('/').rstrip('/v1')}/v1/chat/completions"

                    # Log request info (without sensitive data)
                    masked_headers = dict(self.http_headers)
                    if "Authorization" in masked_headers:
                        masked_headers["Authorization"] = "Bearer sk-****"
                    logging.getLogger(self.valves.APP_ID).debug(
                        f"_get_summary: Sending request to {url} with masked headers {masked_headers}"
                    )

                    try:
                        # Create a session with proper retry handling
                        session = requests.Session()
                        session.trust_env = False  # Ignore environment variables
                        retries = Retry(
                            total=max_retries,
                            backoff_factor=wait_interval,
                            status_forcelist=[429, 500, 502, 503, 504],
                            allowed_methods=frozenset(['POST'])  # Add POST method
                        )
                        adapter = HTTPAdapter(max_retries=retries)
                        session.mount('http://', adapter)
                        session.mount('https://', adapter)

                        # Try primary service
                        response = session.post(
                            url=url,
                            headers=self.http_headers,
                            json=summarization_payload,
                            timeout=60,
                            proxies={"http": "", "https": ""}  # Explicitly bypass proxies
                        )

                        # Process response as before
                        if response.status_code == 200:
                            res = response.json()
                            summary = res["choices"][0]["message"]["content"]
                        elif response.status_code == 401:
                            logging.getLogger(self.valves.APP_ID).error(f"_get_summary: Authentication error: {response.text}")
                            summary = "Error: Authentication failed with the LLM service. Please check your API key."
                        else:
                            logging.getLogger(self.valves.APP_ID).error(
                                f"_get_summary: HTTP Error {response.status_code}: {response.text}"
                            )
                            summary = f"_get_summary: Error: Service returned status code {response.status_code}"
                    except requests.exceptions.ConnectionError as e:
                        # Connection failed - log the issue
                        logging.getLogger(self.valves.APP_ID).error(
                            f"Failed to connect to LiteLLM at {self.service_url}: {e}"
                        )
                        summary = "Error: Could not connect to LLM service. Please try again later."
                    except Exception as e:
                        logging.getLogger(self.valves.APP_ID).error(f"_get_summary: Error processing text: {e}")
                        summary = f"Error processing text"

        return summary

    def _check_litellm_status(self) -> bool:
        """Check if LiteLLM server is running"""
        url = f"{self.service_url.rstrip('/').rstrip('/v1')}/health"

        try:
            # Disable any proxy settings that might interfere
            local_session = requests.Session()
            local_session.trust_env = False  # Ignore environment variables

            response = local_session.get(
                url=url,
                headers=self.http_headers,
                timeout=20,
                proxies={"http": "", "https": ""}  # Explicitly bypass proxies
            )
            logging.getLogger(self.valves.APP_ID).debug(f"_check_litellm_status: LiteLLM status check: {response.status_code}")
            return response.status_code == 200
        except requests.exceptions.ConnectionError as e:
            logging.getLogger(self.valves.APP_ID).error(f"_check_litellm_status: Failed to connect to LiteLLM at {url}: {e}")
            logging.getLogger(self.valves.APP_ID).error(f"_check_litellm_status: LiteLLM service is not running or unreachable.")
            return False