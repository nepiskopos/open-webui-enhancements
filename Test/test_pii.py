from argparse import ArgumentParser
from typing import Any
import json
import mimetypes
import os
import requests


def is_docx_file(file_path: str) -> bool:
    """
    Check if the file is a valid DOCX file by checking its MIME type.

    Args:
        file_path: Path to the file to check

    Returns:
        True if the file is a valid DOCX file, False otherwise
    """
    # Check if path is valid before accessing file
    if not file_path or not os.path.exists(file_path) or not os.path.isfile(file_path):
        return False

    # Get the MIME type
    mime_type, _ = mimetypes.guess_type(file_path)

    # Check for DOCX MIME type
    return mime_type == 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'

def upload_file(url: str, token: str, file_path: str, timeout: int = 30) -> dict[str, Any]:
    """
    Upload a file to the Open WebUI API and return the upload information.

    Args:
        url: Base URL of the Open WebUI API
        token: Authentication token
        file_path: Path to the file to upload
        timeout: Request timeout in seconds

    Returns:
        Dictionary containing the upload information

    Raises:
        FileNotFoundError: If the file doesn't exist
        ValueError: If URL or token is invalid
        requests.RequestException: For API communication errors
    """
    if not url:
        raise ValueError("API URL cannot be empty")
    if not token:
        raise ValueError("Authentication token cannot be empty")
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    api_url = f'{url.rstrip("/")}/api/v1/files/'
    headers = {
        'Authorization': f'Bearer {token}',
        'Accept': 'application/json'
    }

    try:
        with open(file_path, 'rb') as file_obj:
            files = {'file': (os.path.basename(file_path), file_obj)}
            response = requests.post(
                api_url,
                headers=headers,
                files=files,
                timeout=timeout
            )

        response.raise_for_status()  # Raise an exception for HTTP errors
        return response.json()

    except requests.RequestException as e:
        print(f"Error uploading file: {e}")
        raise

def parse_sse_response(response_bytes: bytes, return_all: bool = False) -> list | dict | None:
    """
    Parse API response bytes into structured JSON data, handling both SSE and regular JSON formats.

    This function identifies the format of the response and extracts the content appropriately.

    Args:
        response_bytes: The raw bytes response from the API
        return_all: If True, returns all content chunks as a list; otherwise returns only the first chunk

    Returns:
        Parsed JSON data from the response, or None if parsing failed
    """
    print("Response bytes:", response_bytes[:100], "..." if len(response_bytes) > 100 else "", "\n")

    try:
        # Convert bytes to string
        response_text = response_bytes.decode('utf-8')

        # Check if this is an SSE response or regular JSON
        if response_text.startswith('data:'):
            # Process as SSE (Server-Sent Events)
            chunks = response_text.strip().split('\n\n')
            results = []

            for chunk in chunks:
                if not chunk.startswith('data:') or chunk == 'data: [DONE]':
                    continue

                json_str = chunk[6:]  # Remove 'data: ' prefix
                try:
                    parsed_json = json.loads(json_str)
                    if 'choices' in parsed_json and parsed_json['choices']:
                        choice = parsed_json['choices'][0]
                        if 'delta' in choice and 'content' in choice['delta']:
                            content_str = choice['delta'].get('content')
                            if content_str:
                                parsed_content = json.loads(content_str)
                                results.append(parsed_content)
                                if not return_all:
                                    return parsed_content
                except json.JSONDecodeError:
                    continue

            # Return results based on mode
            if return_all and results:
                return results
            elif results:
                return results[0]
        else:
            # Process as regular JSON response
            parsed_json = json.loads(response_text)
            if 'choices' in parsed_json and parsed_json['choices']:
                choice = parsed_json['choices'][0]
                if 'message' in choice and 'content' in choice['message']:
                    content_str = choice['message'].get('content')
                    if content_str:
                        return json.loads(content_str)

    except Exception as e:
        print(f"Error parsing response: {e}")

    return None

def get_file_pii(
    url: str,
    token: str,
    model: str,
    files: list[dict[str, Any]],
) -> dict | list | None:
    """
    Send a request to the Open WebUI API to get PIIs within the content of uploaded files.

    Args:
        url: Base URL of the Open WebUI API
        token: Authentication token
        model: Model ID to use for identifying PIIs
        files: List of file information dictionaries

    Returns:
        Parsed JSON response data, or None if the request failed

    Raises:
        ValueError: If URL, token, model or files are invalid
        requests.RequestException: For API communication errors
    """
    # Validate inputs
    if not url:
        raise ValueError("API URL cannot be empty")
    if not token:
        raise ValueError("Authentication token cannot be empty")
    if not model:
        raise ValueError("Model ID cannot be empty")
    if not files:
        raise ValueError("File information cannot be empty")

    # Set default prompt (or set it to an empty string)
    prompt = "Please, identify any PIIs within the content of the uploaded file."

    # Prepare API endpoint
    api_url = f'{url.rstrip("/")}/api/chat/completions'

    headers = {
        'Authorization': f'Bearer {token}',
        'Content-Type': 'application/json'
    }

    # Prepare request payload
    payload = {
        'model': model,
        'messages': [
            {
                'role': 'user',
                'content': prompt
            }
        ],
        'files': files,
        'stream': False
    }

    try:
        # Send request to API
        response = requests.post(
            api_url,
            headers=headers,
            json=payload,
            timeout=60
        )

        # Check response status
        if response.status_code != 200:
            print(f"Error: API returned status code {response.status_code}")
            print(f"Response: {response.text}")
            response.raise_for_status()
            return None

        # Process response
        result = parse_sse_response(response.content)
        return result

    except requests.RequestException as e:
        print(f"Error identifying PII in file: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response content: {e.response.text}")
        raise

def main():
    """
    Main function to handle command-line arguments and orchestrate the PII detection process.
    """
    # Parse command-line arguments
    parser = ArgumentParser(
        prog='PiiTest',
        description='Test the PII detection within a DOCX file content using the PII Pipeline (+ Filter) via Open WebUI API.',
    )
    parser.add_argument('-u', '--url', metavar='URL', help='Open WebUI base URL', type=str, required=True)
    parser.add_argument('-t', '--token', metavar='TOKEN', help='Access token for Open WebUI', type=str, required=True)
    parser.add_argument('-m', '--model', metavar='MODEL', default="pii-pipeline",
                        help='Model to use for PII detection (default: pii-pipeline)', type=str)
    parser.add_argument('-f', '--file', metavar='FILE', help='Path to the DOCX file to process for PII detection', type=str, required=True)
    parser.add_argument('-o', '--output', metavar='OUTPUT', help='Optional file to save the detection result', type=str)
    args = parser.parse_args()

    # Validate and process file path
    try:
        file_path = args.file if os.path.isabs(args.file) else os.path.join("docx_files", args.file)

        print(f"Processing file: {file_path}")

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File {file_path} does not exist")

        if not os.path.isfile(file_path):
            raise ValueError(f"{file_path} is not a file")

        if not is_docx_file(file_path):
            raise ValueError(f"{file_path} is not a valid DOCX file")

        # Upload the file to Open WebUI
        print("Uploading file to Open WebUI API...")
        upload_info = upload_file(args.url, args.token, file_path)

        # Ensure we have the proper content type
        if not upload_info["meta"]["content_type"]:
            upload_info["meta"]["content_type"] = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"

        print(f"File uploaded successfully with ID: {upload_info['id']}")

        # Prepare file info for PII detection
        files = [
            {
                "type": "file",
                "file": upload_info
            }
        ]

        # Request PII detection
        print("Requesting PII detection...")
        result = get_file_pii(args.url, args.token, args.model, files)

        # Process and display results
        if result:
            print("\nDetection Result:")
            print("=" * 80)

            # Extract the detected PII
            pii = result[0]["pii"] if isinstance(result, list) else result["pii"]
            print(pii)
            print("=" * 80)

            # Save to output file if requested
            if args.output:
                with open(args.output, 'w', encoding='utf-8') as f:
                    f.write(pii)
                print(f"\nDetected PII saved to: {args.output}")

            # Return success code
            return 0
        else:
            print("Error: Failed to detect PII in the file.")
            return 1

    except (FileNotFoundError, ValueError) as e:
        print(f"Error: {e}")
        return 1
    except requests.RequestException as e:
        print(f"API Error: {e}")
        return 1
    except Exception as e:
        print(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)