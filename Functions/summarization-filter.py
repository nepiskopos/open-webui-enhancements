from pydantic import BaseModel, Field
from typing import Optional
import errno
import json
import logging
import os
import sys


class Filter:
    '''
    This is a class for creating a Filter function for assisting
    with summarizing documents using LangGraph. It modifies user
    input before forwarding the request to the model, and modifies
    the model response before displaying it to the user. It also
    deletes user-uploaded files after the model response is modified.
    '''
    class Valves(BaseModel):
        APP_ID: str = Field(
            default=os.getenv(
                "APP_ID",
                "FILTER_DOCUMENT_SUMMARIZATION_LANGGRAPH",
            ),
            description="Application name for logging.",
        )

    def __init__(self):
        ''' Constructor method '''
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

        # Initialize the default Open WebUI file upload directory
        self.default_file_upload_path = "/app/backend/data/uploads"

    async def on_valves_updated(self):
        '''This function is called when the valves are updated.'''
        pass

    async def inlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies form data and user input before forwarding the request to the model.'''
        logging.getLogger(self.valves.APP_ID).debug(f"INLET begin")
        # logging.getLogger(self.valves.APP_ID).debug(f"INLET begin:\nBody:\n{json.dumps(body, indent=2)}")

        # Insert desired file information into the request body
        body["messages"][-1]['content'] = json.dumps({
            'user_message': body["messages"][-1]['content'],
            'chat_id': body.get('metadata', {}).get('chat_id', ''),
        })

        logging.getLogger(self.valves.APP_ID).debug(f"INLET end")

        return body

    async def outlet(self, body: dict, __user__: Optional[dict] = None) -> dict:
        '''Modifies model response form data before returning them to the user.'''
        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET begin")
        # logging.getLogger(self.valves.APP_ID).debug(f"OUTLET begin:\nBody:\n{json.dumps(body, indent=2)}")

        # If the model response contains an error message, log it
        if 'ERROR: ' in body.get('messages', [])[-1]['content']:
            logging.getLogger(self.valves.APP_ID).error(f"OUTLET: Model response contains an error: {body.get('messages', [])[-1]['content']}")
        else:
            # logging.getLogger(self.valves.APP_ID).debug(f"OUTLET: Model response content: {body.get('messages', [])[-1]['content']}")

            result = json.loads(body.get('messages', [])[-1]['content'])

            content = ''

            for file_info in result:
                if file_info.get('id', '') and file_info.get('filename', '') and file_info.get('summary', ''):
                    content += f'### Summary for file **{file_info["filename"].strip()}**:\n{file_info["summary"].strip()}\n\n---\n'

            if content:
                content = content[:-4].strip()
                body['messages'][-1]['content'] = content

            self._fs_delete_files([os.path.join(self.default_file_upload_path, f"{file_info['id']}_{file_info['filename']}") for file_info in result])

        logging.getLogger(self.valves.APP_ID).debug(f"OUTLET end")

        return body

    def _fs_delete_files(self, file_paths: list[str]) -> None:
        '''
        Delete files from file system

        Input parameters:
        * file_paths: A list with the file paths to be deleted.
        '''
        for path in file_paths:
            try:
                # Remove other user-uploaded files
                os.remove(path)
            except FileNotFoundError:
                logging.getLogger(self.valves.APP_ID).error(f'''_fs_delete_files: File '{path}' not found.''')
            except PermissionError:
                logging.getLogger(self.valves.APP_ID).error(f'''_fs_delete_files: Permission denied to delete '{path}'.''')
                raise
            except OSError as e:
                if e.errno == errno.EISDIR:
                    logging.getLogger(self.valves.APP_ID).error(f'''_fs_delete_files: '{path}' is a directory, not a file.''')
                else:
                    logging.getLogger(self.valves.APP_ID).error(f'''_fs_delete_files: An unexpected error occurred while trying to delete '{path}': {e}''')
                    raise