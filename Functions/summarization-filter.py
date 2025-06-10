from pydantic import BaseModel, Field
from typing import Optional
import json
import logging
import os
import sys
import threading


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

    def get_acceptable_user_files(self, user_id: str, chat_id: str) -> dict:
        return_data = {}
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for retrieving data
            if key in self._data:
                # Return user-uploaded acceptable files
                return_data = self._data[key]['acceptable']
        return return_data

    def get_other_user_files(self, user_id: str, chat_id: str) -> dict:
        return_data = {}
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for retrieving data
            if key in self._data:
                # Return user-uploaded "other" files
                return_data = self._data[key]['other']
        return return_data

    def get_user_files(self, user_id: str, chat_id: str) -> dict:
        return_data = {}
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for retrieving data
            if key in self._data:
                # Return all user-uploaded files
                return_data = self._data[key]
        return return_data

    def insert_acceptable_user_files(self, user_id: str, chat_id: str, files: dict) -> None:
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for inserting data
            if key not in self._data:
                # User ID does not exist in the dictionary - create an entry for it
                self._data[key] = {'acceptable': {}, 'other': {}}
            # Insert new user-uploaded acceptable files in the dictionary
            self._data[key]['acceptable'].update(files)

    def insert_other_user_files(self, user_id: str, chat_id: str, files: dict) -> None:
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for inserting data
            if key not in self._data:
                # User ID does not exist in the dictionary - create an entry for it
                self._data[key] = {'acceptable': {}, 'other': {}}
            # Insert new user-uploaded "other" files in the dictionary
            self._data[key]['other'].update(files)

    def insert_user_files(self, user_id: str, chat_id: str, files: dict) -> None:
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for inserting data
            if key not in self._data:
                # user_id exists in the dictionary - insert user_id into the dictionary
                self._data[key] = {'acceptable': {}, 'other': {}}
            # Insert new user-uploaded acceptable and "other" files in the dictionary
            self._data[key]['acceptable'].update(files['acceptable'])
            self._data[key]['other'].update(files['other'])

    def delete_acceptable_user_files(self, user_id: str, chat_id: str) -> None:
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for writing
            if key in self._data:
                # Remove user-uploaded acceptable files from the dictionary
                del self._data[key]['acceptable']

    def delete_other_user_files(self, user_id: str, chat_id: str) -> None:
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for writing
            if key in self._data:
                # Remove user-uploaded "other" files from the dictionary
                del self._data[key]['other']

    def delete_user_files(self, user_id: str, chat_id: str) -> None:
        key = f'{user_id}_{chat_id}'
        with self._lock:  # Acquire the lock for writing
            if key in self._data:
                # Remove all user-related entries from the dictionary
                del self._data[key]['acceptable']
                del self._data[key]['other']
                del self._data[key]

    def get_all_data(self) -> dict:
        with self._lock:  # Acquire the lock for writing
            return dict(self._data)


class Filter:
    '''
    This is a class for creating a Filter function for
    presenting the summaries of user-uploaded DOCX files.
    '''
    class Valves(BaseModel):
        APP_ID: str = Field(
            default=os.getenv(
                "APP_ID",
                "FILTER_DOCUMENT_SUMMARIZATION",
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

        # Initialize the path to the default Open WebUI file upload directory
        self.default_file_upload_path = "/app/backend/data/uploads"

        # Keep a list of uploaded files
        self.user_uploaded_files = SharedUserFilesDict()

        # Keep a list of latest users upload timestamps
        self.user_timestamps = SharedUserFilesLatestUploadDict()

    async def on_valves_updated(self):
        '''This function is called when the valves are updated.'''
        pass

    async def inlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies form data and user input before forwarding the request to the model.'''
        logging.getLogger(self.valves.APP_ID).debug(f"INLET begin:\nBody:\n{json.dumps(body, indent=2)}")

        # Extract user chat, session and files information from the request body
        body_files = body.get("files", [])
        chat_id = body.get('metadata', {}).get('chat_id', '')

        # Extract user ID
        user_id = __user__['id']

        # Remove user-uploaded files from the shared dictionary (if any)
        self.user_uploaded_files.delete_user_files(user_id, chat_id)

        if body_files:
            # Retrieve the timestamp of the user's latest file uploaded
            latest_timestamp = self.user_timestamps.get_user_latest_timestamp(user_id, chat_id)

            # Check if no timestamp has been set yet
            if latest_timestamp == 0:
                # Extract the timestamp of the latest model update
                latest_timestamp = int(body.get('metadata', {}).get('model', {}).get('created', 0))

                # Update the latest timestamp of the user in the shared dictionary
                self.user_timestamps.update_user_latest_timestamp(user_id, chat_id, latest_timestamp)

            # Keep a copy of the timestamp of the user's latest file uploaded
            new_latest_timestamp = latest_timestamp


            user_files = {'acceptable': {}, 'other': {}}

            try:
                # Iterate over the whole list of uploaded files
                for file_info in body_files:
                    # Extract file information
                    file = file_info["file"]

                    # Construct input file path in storage
                    file_path = os.path.join(
                        self.default_file_upload_path, f'{file["id"]}_{file["filename"]}'
                    )

                    # Check if file exists in the file system
                    if os.path.exists(file_path) and os.path.isfile(file_path):
                        # Check if input file type is an acceptable document
                        if file['meta']['content_type'] == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document':
                            # Add file ID to the list of collected file IDs
                            user_files['acceptable'][file["id"]] = {
                                "id": file["id"],
                                "filename": file["filename"],
                                "filepath": file_path,
                            }
                        else:
                            # Add file ID to the list of collected file IDs
                            user_files['other'][file["id"]] = {
                                "id": file["id"],
                                "filename": file["filename"],
                                "filepath": file_path,
                            }

                        if int(file["created_at"]) > new_latest_timestamp:
                            # Update the latest timestamp of the uploaded files
                            new_latest_timestamp = int(file["created_at"])

                logging.getLogger(self.valves.APP_ID).debug(f"INLET: User-uploaded files '{json.dumps(user_files, indent=2)}'")
            except Exception as e:
                logging.getLogger(self.valves.APP_ID).error(f"INLET: Exception while processing user-uploaded files: {e}")

            # Update the latest timestamp of the user in the shared dictionary
            self.user_timestamps.update_user_latest_timestamp(user_id, chat_id, new_latest_timestamp)

            # Insert new user-uploaded files into the shared dictionary
            self.user_uploaded_files.insert_user_files(user_id, chat_id, user_files)

            logging.getLogger(self.valves.APP_ID).debug(f"INLET: User-uploaded files:\n{json.dumps(user_files, indent=2)}")

            # Keep only new and acceptable files in the request body (removing previously processed and unacceptable files)
            body["files"] = [file_info for file_info in body["files"] if file_info["file"]["id"] in user_files['acceptable'].keys()]
            body["metadata"]["files"] = [file_info for file_info in body["metadata"]["files"] if file_info["file"]["id"] in user_files['acceptable'].keys()]

        logging.getLogger(self.valves.APP_ID).debug(f"INLET end")

        return body

    async def outlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies model response form data before returning them to the user.'''
        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET begin:\nBody:\n{json.dumps(body, indent=2)}")

        # Initialize an empty string for the prompt response
        prompt_response = ""

        # Extract user chat information
        chat_id = body.get('chat_id', '')

        # Extract user ID
        user_id = __user__['id']

        # Retrieve user-uploaded files from the shared dictionary
        user_files = self.user_uploaded_files.get_user_files(user_id, chat_id)

        # Get content from model response
        model_response_content = body.get('messages', [])[-1].get('content', '')

        # logging.getLogger(self.valves.APP_ID).debug(f"OUTLET: Received files summaries:\n{json.dumps(model_response_content, indent=2)}")

        try:
            # Get response content (substitutions) as a JSON object
            summaries = json.loads(model_response_content)

            # Check if text in any user-uploaded acceptable files was processed
            if summaries:
                # Create a list for collecting file IDs of the acceptable files which where successfully summarized
                processed_file_ids = []

                # Iterate over the list of modified files
                for summary in summaries:
                    if summary['summary']:
                        user_files['acceptable'][summary['id']]['summary'] = summary['summary']

                        # Collect file ID
                        processed_file_ids.append(summary['id'])

                if processed_file_ids:
                    # Create message output for summarized files
                    prompt_response = prompt_response + \
                        "The following DOCX files have been successfully summarized:\n\n" + \
                        "\n\n\n".join([f"* **{v['filename']}**:\n{v['summary']}" for v in user_files['acceptable'].values()])

                    # Collect file IDs of acceptable files whose summarization failed
                    unprocessed_file_ids = set(user_files['acceptable'].keys()).difference(set(processed_file_ids))

                    if unprocessed_file_ids:
                        # Add additional message output for failed files
                        if prompt_response:
                            prompt_response = prompt_response + "\n\n---\n"

                        prompt_response = prompt_response + \
                            "\n\nThe following DOCX files could not be processed due to some system error:\n\n" + \
                            "\n".join([f"- **{v['filename']}**" for k, v in user_files['acceptable'].items() if k in unprocessed_file_ids])
                else:
                    prompt_response = "The uploaded DOCX files could not be processed due to some system error."
            else:
                prompt_response = "No summaries were created."

            if user_files['other']:
                # Add additional message output for non-acceptable files
                prompt_response = prompt_response + \
                    "\n\n---\nThe following uploaded files could not be processed" + \
                    " because this model only supports DOCX files with UTF-8 encoding:\n\n"
                prompt_response = prompt_response + \
                    "\n".join([f"* **{v['filename']}**" for v in user_files['other'].values()])
        except SyntaxError as e:
            logging.getLogger(self.valves.APP_ID).error(f"OUTLET: Response to the HTTP request sent to the pipeline is not a valid syntax object: {e}")
            prompt_response = "A server error occurred while processing the uploaded DOCX files."
        except TypeError as e:
            logging.getLogger(self.valves.APP_ID).error(f"OUTLET: Response to the HTTP request sent to the pipeline is of wrong type: {e}")
            prompt_response = "A server error occurred while processing the uploaded DOCX files."
        except json.JSONDecodeError as e:
            logging.getLogger(self.valves.APP_ID).error(f"OUTLET: Response to the HTTP request sent to the pipeline is not a valid JSON object: {e}")
            logging.getLogger(self.valves.APP_ID).error(f"OUTLET: Erroneous pipeline response: {prompt_response}")
            if model_response_content == None:
                prompt_response = "A server error occurred while processing the uploaded DOCX files."
            else:
                prompt_response = model_response_content
        except Exception as e: # Catch all other exceptions.
            logging.getLogger(self.valves.APP_ID).error(f"OUTLET: An unexpected error occurred while processing files: {e}")
            prompt_response = "A server error occurred while processing the uploaded DOCX files - some files failed to be processed."

        if prompt_response:
            # Update the last chat message (what the user will percieve as the model response) with the formatted response
            body['messages'][-1]['content'] = prompt_response

        # Delete user-uploaded acceptable files from the file system
        self._fs_delete_files([v['filepath'] for v in user_files['acceptable'].values()])

        # Delete user-uploaded 'other' files from the file system
        self._fs_delete_files([v['filepath'] for v in user_files['other'].values()])

        # Remove user-uploaded files from the shared dictionary (if any)
        self.user_uploaded_files.delete_user_files(user_id, chat_id)

        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET end")

        return body

    def _fs_delete_files(self, file_paths: list[str]) -> None:
        '''
        Delete files from file system

        Input parameters:
        * file_paths: A list with the file paths to be deleted.
        '''
        for path in file_paths:
            if path:
                try:
                    if os.path.exists(path):
                        if os.path.isfile(path):
                            os.remove(path)
                except TypeError as e:
                    logging.getLogger(self.valves.APP_ID).error(f"_fs_delete_files: File '{path}' is invalid: {e}")
                except FileNotFoundError as e:
                    logging.getLogger(self.valves.APP_ID).error(f"_fs_delete_files: File '{path}' not found: {e}")
                except UnicodeEncodeError as e:
                    logging.getLogger(self.valves.APP_ID).error(f"_fs_delete_files: File '{path}' contains invalid characters: {e}")
                except PermissionError as e:
                    logging.getLogger(self.valves.APP_ID).error(f"_fs_delete_files: Permission denied to delete '{path}': {e}")
                except OSError as e:
                    logging.getLogger(self.valves.APP_ID).error(f"_fs_delete_files: An unexpected error occurred while trying to delete '{path}': {e}")