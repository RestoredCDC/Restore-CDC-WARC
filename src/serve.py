#!/usr/bin/env python3

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

def rewrite_html_urls(full_path, content):
    """
    Rewrite any of these to point to ourselves, as needed

    <a      href="...">
    <link   href="...">
    <script src="...">
    <img    src="...">

    :param full_path: The full path, to find the relevant (sub)domain
    :param content: The HTML content as bytes
    :return: Rewritten (if needed) HTML content as bytes

    The proper way will be to parse the HTML, and then cross our
    fingers there is nothing in the HTML comments to parse, but for
    now the initial version this does a broad set of text replaces.

    There are also places with references to the original websites in
    srcset attributes and such, so there are no doubt other hidden
    treasures.
    """
    path_match = re.match(r"https://([^/]+)/", full_path)
    if not path_match:
        # Can't do anything useful here
        return content
    website = path_match.group(1)

    # Looks like the hivrisk.cdc.gov site had some stray references to their staging site,
    # not that it makes much of a difference :)
    content = content.replace(b"hivriskstage.cdc.gov", b"hivrisk.cdc.gov")

    content = content.replace(bytes(f" https://{website}/", "utf-8"),
                              bytes(f" /https://{website}/", "utf-8"))
    content = content.replace(b"href=\"/", bytes(f"href=\"/https://{website}/", "utf-8"))
    content = content.replace(bytes(f"href=\"https://{website}/", "utf-8"),
                              bytes(f"href=\"/https://{website}/", "utf-8"))
    content = content.replace(bytes(f"href=\'https://{website}/", "utf-8"),
                              bytes(f"href=\'/https://{website}/", "utf-8"))
    content = content.replace(bytes(f"src=\"https://{website}/", "utf-8"),
                              bytes(f"src=\"/https://{website}/", "utf-8"))
    content = content.replace(bytes(f"src=\'https://{website}/", "utf-8"),
                              bytes(f"src=\'/https://{website}/", "utf-8"))

    return content

def find_content(full_path):
    """
    Find the relevant content, possibly adding or removing an extra '/'

    :param full_path: The original URL, as bytes
    :return: Two values, the content (as bytes), ans the mimetype (as str)
    """
    raw_key = bytes(full_path, "utf-8")

    logging.debug(f"Looking up key: {full_path}")
    content = content_db.get(raw_key)
    mimetype_bytes = mimetype_db.get(raw_key)

    if content is None or mimetype_bytes is None:
        # Try with, or without a / at the end
        key_match = re.match(r"([^?]+)/\?(.*)$", full_path)
        if key_match:
            raw_key = bytes(key_match.group(1) + "?" + key_match.group(2), "utf-8")
        else:
            key_match = re.match(r"([^?]+)\?(.*)$", full_path)
            if key_match:
                raw_key = bytes(key_match.group(1) + "/?" + key_match.group(2), "utf-8")
            else:
                raw_key = raw_key + b"/"
        logging.debug(f"Looking up secondary key: {raw_key}")
        content = content_db.get(raw_key)
        mimetype_bytes = mimetype_db.get(raw_key)
        if content is None or mimetype_bytes is None:
            logging.warning(f"Missing content or mimetype for path: {full_path}")
            return None, None

        logging.debug(f"Found {raw_key} after modification")

    mimetype = mimetype_bytes.decode("utf-8")

    return content, mimetype

@app.route("/")
def home():
    """
    Default route
    """
    return redirect("/www.cdc.gov/")

@app.route("/<path:subpath>")
def lookup(subpath):
    """
    Catch-all route
    """
    try:
        if request.query_string:
            full_path = request.full_path[1:]
        else:
            full_path = request.path[1:]
        logging.debug(f"Full path: {full_path}")
        # Fix missing slash if needed
        if full_path.startswith("https:/") and not full_path.startswith("https://"):
            full_path = full_path.replace("https:/", "https://", 1)
            logging.debug(f"Full path fixed: {full_path}")

        content, mimetype = find_content(full_path)
        raw_key = bytes(full_path, "UTF-8")

        if content is None or mimetype is None:
            return Response("Not Found", status=404)

        if mimetype == "=redirect=":
            redirect_target = content.decode("utf-8")
            logging.info(f"Redirecting from {full_path} to {redirect_target}")
            return redirect(f'/{redirect_target}')

        logging.debug(f"Mime type {mimetype} for {full_path}")

        if full_path.startswith("https://") and mimetype == "text/html" or mimetype.startswith("text/html;"):
            content = rewrite_html_urls(full_path, content)

        # XXX: We probably also need to rewrite URLs in javascript and
        #      stylesheets that we serve up

        return Response(content, mimetype=mimetype)

    except Exception as e:
        logging.exception(f"Error retrieving {subpath}")
        return Response("Internal Server Error", status=500)


if __name__ == "__main__":
    print(f"Starting cdcmirror server process at port {serverPort}")
    serve(app, host=hostName, port=serverPort)
