import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0] # if I put it in the root
print(f"Current file: {__file__}")
print(f"BASE_DIR: {BASE_DIR}")
print(f"FFMPEG_BIN env: {os.getenv('FFMPEG_BIN')}")

api_file = Path("api/content_api.py").resolve()
print(f"API file: {api_file}")
api_base_dir = api_file.parents[1]
print(f"API BASE_DIR: {api_base_dir}")
