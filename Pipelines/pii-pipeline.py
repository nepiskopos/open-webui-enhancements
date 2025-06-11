from pydantic import BaseModel, Field
from requests.adapters import HTTPAdapter
from typing import Generator, Iterator, Optional, Union
from urllib3.util.retry import Retry
import json
import logging
import os
import re
import requests
import sys
import threading


class OIFile:
    '''
    This is a class for representing a user-uploaded
    file object. It stores the file ID, the file name,
    the file content and the length of the fiel content.
    '''
    def __init__(self, id: str, name: str, content: str):
        self.id = id
        self.name = name
        self.content = self._build_content(content)
        self.size = len(self.content)

    def get_id(self) -> str:
        return self.id

    def get_name(self) -> str:
        return self.name

    def get_content(self) -> str:
        return self.content

    def get_size(self) -> int:
        return self.size

    def _build_content(self, text: str) -> str:
        # First normalize consecutive newlines to single newlines
        document = re.sub(r'\n+', ' \n', text)

        # Replace tabs with spaces
        document = re.sub(r'\t', ' ', document)

        # Replace multiple consecutive spaces with a single space
        document = re.sub(r' +', ' ', document)

        return document

    def __repr__(self) -> str:
        return f"File(id={self.id}, name={self.name}, size={self.size} bytes)"


class SharedUserFilesDict:
    '''
    This is a class for creating a shared dictionary
    for storing information on the latest user-uploaded
    files. It uses a lock to ensure thread safety when
    accessing the shared dictionary.
    '''
    def __init__(self):
        self._data = dict()
        self._lock = threading.Lock()  # Create a lock

    def get_user_files(self, user_id: str, chat_id: str) -> dict:
        return_data = {}

        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for retrieving data
            # Check if the key exists in the dictionary
            if key in self._data:
                return_data = self._data[key]

        return return_data

    def insert_user_files(self, user_id: str, chat_id: str, files: dict) -> None:
        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for inserting data
            # Check if the key exists in the dictionary
            if key not in self._data:
                self._data[key] = {}
            for k, v in files.items():
                if k not in self._data[key]:
                    self._data[key][k] = v

    def delete_user_data(self, user_id: str, chat_id: str) -> None:
        # Construct the key for the user_id and chat_id
        key = f'{user_id}_{chat_id}'

        with self._lock:  # Acquire the lock for writing
            # Check if the user_id exists in the dictionary
            if key in self._data:
                # Remove user data from the dictionary
                del self._data[key]

    def get_all_data(self) -> dict:
        with self._lock:  # Acquire the lock for writing
            return dict(self._data)


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
    sensitive information in text originating from user=uploaded
    DOCX document files.
    '''
    class Valves(BaseModel):
        LITELLM_API_BASE_URL: str = Field(
            default=os.getenv(
                "LITELLM_API_BASE_URL",
                "http://host.docker.internal:4000/v1",
            ),
            description="URL of the LiteLLM service.",
        )
        LITELLM_API_KEY: str = Field(
            default=os.getenv(
                "LITELLM_API_KEY",
                "sk-1234",
            ),
            description="API key for authenticating requests to the LiteLLM service.",
        )
        MODEL_ID: str = Field(default=os.getenv(
                "MODEL_ID",
                "GPT-4o",
            ),
            description="Model of choice as defined in LiteLLM configuration.",
        )
        APP_ID: str = Field(
            default=os.getenv(
                "APP_ID",
                "PIPELINE_DOCUMENT_GDPR_PII_IDENTIFICATION",
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

        # Initialize LLM endpoint API URL
        self.service_url = f"{self.valves.LITELLM_API_BASE_URL.rstrip('/').rstrip('/v1')}/v1/chat/completions"

        # Initialize HTTP headers for the LLM endpoint API requests
        self.http_headers = {
            "Authorization": f"Bearer {self.valves.LITELLM_API_KEY}",
            "Content-Type": "application/json",
        }

        self.system_prompt = '''
            You are a GDPR-compliant data privacy assistant. Your role is to detect Personally Identifiable Information (PII) in user-provided text based on the EU’s General Data Protection Regulation (GDPR).

            PII includes any information relating to an identified or identifiable natural person, either directly (e.g., name, email address, national ID number) or indirectly (e.g., IP address, location data, unique device identifiers, or any data that can identify a person when combined with other information).

            When analyzing documents, you must:
            1. Identify and extract all PII instances.
            2. Categorize each instance.
            3. Determine if it is a direct or indirect identifier.
            4. Justify the classification based on GDPR definitions.

            Output results in structured JSON format, suitable for downstream processing.
            Maintain strict compliance with GDPR’s definition of personal data as described in Article 4(1).
        '''

        self.user_prompt_template = '''
            ### Instruction:
            Analyze the following document and identify all instances of Personally Identifiable Information (PII) according to the EU's GDPR.

            ### Input:
            {text}

            ### Response:
            For each identified PII instance, return the:
            - text: The extracted text, exactly as it appears in the document
            - category: The PII category (e.g., name, email, phone number, IP address, health data)
            - type: The PII identifier type (direct or indirect)
            - justification: The justification for PII classification

            Format your results as a structured JSON array, where each object represents one PII instance.

            Example:
            [
                {{"text": "John Doe", "category": "name", "type": "direct", "justification": "Identifies an individual directly."}},
                {{"text": "d.joe@brand.co", "category": "email", "type": "direct", "justification": "Identifies an individual directly through their email address."}}
            ]
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


            new_files = {}

            # Extract file info for all files in the body
            for file in self._extract_body_files(file_infos):
                # Add file to the list of collected new files
                new_files[file.get_id()] = file

            logging.getLogger(self.valves.APP_ID).debug(f"INLET: User-uploaded files '{new_files}'")

            # Keep new user files into the current user file contents
            self.user_file_contents.insert_user_files(user_id, chat_id, new_files)

            # Keep only new and acceptable files in the request body (removing previously processed and unacceptable files)
            body["files"] = [file_info for file_info in body["files"] if file_info["file"]["id"] in new_files]
            body["metadata"]["files"] = [file_info for file_info in body["metadata"]["files"] if file_info["file"]["id"] in new_files]

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
            if not any(user_file.get_size() for user_file in user_files.values()):
                logging.getLogger(self.valves.APP_ID).warning(f"PIPE: All input DOCX files are empty")
                return_data = "All uploaded DOCX files are empty."
            else:
                logging.getLogger(self.valves.APP_ID).debug(f"PIPE: User input files:\n{user_files}")

                data = []

                for file_id, file in user_files.items():
                    if file.get_size():
                        try:
                            logging.getLogger(self.valves.APP_ID).debug(f"PIPE: Getting model file pii...")

                            pii = self._get_pii(file.get_content())

                            data.append(
                                {
                                    "id": file.get_id(),
                                    # "filename": file.get_name(),
                                    "pii": pii,
                                }
                            )
                        except Exception as e:
                            logging.getLogger(self.valves.APP_ID).error(f"PIPE: Error processing file {file.get_name()}: {e}")
                    else:
                        logging.getLogger(self.valves.APP_ID).warning(f"PIPE: File {file.get_name()} is empty")

                if data:
                    return_data = json.dumps(data, indent=2)
                else:
                    return_data = "Error processing the content of uploaded DOCX files."

        logging.getLogger(self.valves.APP_ID).debug(f"PIPE end")

        return return_data

    def _extract_body_files(self, data: dict | list) -> list[OIFile]:
        '''
        This function extracts file information from the provided data.

        It processes the data to create a list of OIFile instances, which represent user-uploaded files.
        The function checks if the input data is a dictionary or a list. If it's a dictionary, it extracts the "files" key.

        If it's a list, it uses the list directly. It then iterates over the entries, checking if the file is a DOCX file
        by verifying the content type. If it is, it creates an OIFile instance for each file and appends it to the list.

        Input parameters:
        * data: A dictionary or list containing file information.
        Returns:
        * A list of OIFile instances representing the user-uploaded files.
        '''
        oifiles = []

        if isinstance(data, dict):
            # If inlet_body is a dictionary (full inlet body), extract the "files" key
            files_entries = data.get("files", [])
        elif isinstance(data, list):
            # If inlet_body is a list (extracted "files" from inlet body), use it directly
            files_entries = data

        for entry in files_entries:
            file_info = entry.get("file", {})
            if file_info:
                # Only process DOCX files
                if file_info['meta']['content_type'] == \
                    'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                    # Create an OIFile instance and append it to the list
                    oifiles.append(
                        OIFile(file_info['id'], file_info['filename'], file_info['data']['content'])
                    )

        return oifiles

    # Change from async function to regular function
    def _get_pii(self, text: str, max_retries: int=5, wait_interval: int=10) -> dict | str:
        '''
        This function sends a request to the OpenAI API to get a the PII within the provided text.
        If the text is empty, it returns a message indicating that no content was provided.
        If an error occurs during the request, it returns an error message.

        Input parameters:
        * text: The text to be analyzed for PII identification.
        Returns:
        * The identified PII.
        '''
        pii = ""

        if not text.strip():
            pii = "No content provided for PII detection."
        else:
            # Check API key before making request
            if not self.valves.LITELLM_API_KEY:
                logging.getLogger(self.valves.APP_ID).error(f"_get_pii: Invalid API key configuration")
                pii = "Error: API key not properly configured. Please set a valid LITELLM_API_KEY environment variable."
            else:
                if self._check_litellm_status():
                    logging.getLogger(self.valves.APP_ID).debug(f"_get_pii: LiteLLM service is running")

                    # Create LLM PII detection payload
                    identification_payload = {
                        "model": self.valves.MODEL_ID,
                        "messages": [
                            {"role": "system", "content": self.system_prompt},
                            {"role": "user", "content": self.user_prompt_template.format(text=text)}
                        ],
                        "temperature": 0.1,
                        "stream": False,
                    }

                    # Log request info (without sensitive data)
                    masked_headers = dict(self.http_headers)
                    if "Authorization" in masked_headers:
                        masked_headers["Authorization"] = "Bearer sk-****"
                    logging.getLogger(self.valves.APP_ID).debug(
                        f"_get_pii: Sending request to {self.service_url} with masked headers {masked_headers}"
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
                            self.service_url,
                            headers=self.http_headers,
                            json=identification_payload,
                            timeout=60,
                            proxies={"http": "", "https": ""}  # Explicitly bypass proxies
                        )

                        # Process response as before
                        if response.status_code == 200:
                            res = response.json()
                            content = res["choices"][0]["message"]["content"]
                            if content.startswith("```json\n") and content.endswith("\n```"):
                                content = content.replace("```json\n", "").replace("\n```", "")
                            pii = json.loads(content)
                        elif response.status_code == 401:
                            logging.getLogger(self.valves.APP_ID).error(f"_get_pii: Authentication error: {response.text}")
                            pii = "Error: Authentication failed with the LLM service. Please check your API key."
                        else:
                            logging.getLogger(self.valves.APP_ID).error(
                                f"_get_pii: HTTP Error {response.status_code}: {response.text}"
                            )
                            pii = f"_get_pii: Error: Service returned status code {response.status_code}"
                    except requests.exceptions.ConnectionError as e:
                        # Connection failed - log the issue
                        logging.getLogger(self.valves.APP_ID).error(
                            f"Failed to connect to LiteLLM at {self.service_url}: {e}"
                        )
                        pii = "Error: Could not connect to LLM service. Please try again later."
                    except json.JSONDecodeError as e:
                        logging.getLogger(self.valves.APP_ID).error(f"_get_pii: JSON decode error: {e}")
                        pii = f"Error processing text: {e}"
                    except Exception as e:
                        logging.getLogger(self.valves.APP_ID).error(f"_get_pii: Error processing text: {e}")
                        pii = f"Error processing text"

        return pii

    def _check_litellm_status(self) -> bool:
        """Check if LiteLLM server is running"""
        url = f"{self.valves.LITELLM_API_BASE_URL.rstrip('/').rstrip('/v1')}/health"

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