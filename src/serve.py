import argparse
import json
import logging
import re
from pathlib import Path
from urllib import parse
from urllib.parse import urlparse

from flask import Flask, Response, redirect, render_template, request, url_for, jsonify
from waitress import serve
import plyvel

# Logging directory setup
LOG_DIR = Path("../logs")
if not LOG_DIR.exists():
    logging.info(f"Creating log directory: {LOG_DIR}")
    LOG_DIR.mkdir(mode=0o755, parents=True)

# Configure logging: logs to both console and a file
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(funcName)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "serve_restoreCDCWarc.log")
    ]
)

parser = argparse.ArgumentParser()
parser.add_argument('--hostname', default="127.0.0.1", type=str)
parser.add_argument('--port', default=7070, type=int)
parser.add_argument('--dbfolder', default="../data/dev/db/cdc_database", type=str)
args = parser.parse_args()

# this is the path of the LevelDB database we converted from .zim using zim_converter.py
db = plyvel.DB(str(args.dbfolder))

# the LevelDB database has 2 keyspaces, one for the content, and one for its type
# please check zim_converter.py script comments for more info
content_db = db.prefixed_db(b"c-")
mimetype_db = db.prefixed_db(b"m-")

app = Flask(__name__)

# serving to localhost interfaces at port 9090
hostName = args.hostname
serverPort = args.port

@app.route("/")
def home():
    """
    """
    return redirect("/www.cdc.gov/")

@app.route("/<path:subpath>")
def lookup(subpath):
    """
    Catch-all route
    """
    try:
        full_path = parse.unquote(subpath)
        logging.debug(f"Full path: {full_path}")
        # Fix missing slash if needed
        if full_path.startswith("https:/") and not full_path.startswith("https://"):
            full_path = full_path.replace("https:/", "https://", 1)
        logging.debug(f"Full path fixed: {full_path}")            
            
        raw_key = bytes(full_path, "UTF-8")

        logging.debug(f"Looking up key: {raw_key}")

        content = content_db.get(raw_key)
        mimetype_bytes = mimetype_db.get(raw_key)

        if content is None or mimetype_bytes is None:
            logging.warning(f"Missing content or mimetype for path: {full_path}")
            return Response("Not Found", status=404)

        mimetype = mimetype_bytes.decode("utf-8")

        if mimetype == "=redirect=":
            redirect_target = content.decode("utf-8")
            logging.info(f"Redirecting from {full_path} to {redirect_target}")
            return redirect(f'/{redirect_target}')

        return Response(content, mimetype=mimetype)

    except Exception as e:
        logging.exception(f"Error retrieving {subpath}")
        return Response("Internal Server Error", status=500)


if __name__ == "__main__":
    print(f"Starting cdcmirror server process at port {serverPort}")
    serve(app, host=hostName, port=serverPort)