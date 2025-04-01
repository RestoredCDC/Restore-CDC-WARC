import gzip
import logging
import os
import re

import plyvel
from warcio.archiveiterator import ArchiveIterator

def create_db(url_list_plus, dbfolder):
    """
    In a given location unpack gzipped files and extract URL and contents into LevelDB
    :param url_list_plus: Hashmap with information about the URLs found at the IA and where they were saved locally
    :param dbfolder: target directory for the LevelDB
    """
    db = plyvel.DB(os.path.join(dbfolder, 'cdc_database'), create_if_missing=True)

    content_db = db.prefixed_db(b"c-")
    mimetype_db = db.prefixed_db(b"m-")

    total_path = 0
    total_url = 0
    for subdomain, paths in url_list_plus.items():
        for url_data in paths:
            total_path += 1
            filename = url_data['fetched']['file']
            if not filename:
                logging.debug(f"Missing warc file for {url_data}")
                continue
            logging.debug(f"Opening file{filename}")
            file_contents = None

            if filename.endswith(".gz"):
                stream = gzip.open(filename, 'rb')
            elif filename.endswith(".warc"):
                stream = open(filename, 'rb')
            else:
                continue

            for record in ArchiveIterator(stream, no_record_parse=False, ensure_http_headers=True):
                if record.rec_type != 'response':
                    if record.rec_type != "warcinfo":
                        logging.debug(f"Record type '{record.rec_type}' skipped.")
                    continue
                uri = record.rec_headers.get_header('WARC-Target-URI')
                payload = record.content_stream().read()
                logging.debug(f"uri = {uri}")
                if uri and payload is not None:
                    if not record.http_headers:
                        logging.info("No http_headers for this warc record")
                        continue

                    urls = url_data['originals']
                    if not (uri in urls):
                        urls.append(uri)
                    # logging.debug(f"urls: {urls}; uri: {uri}; path: {url_data['path']}; originals: {url_data['originals']}")

                    content_type = record.http_headers.get_header('Content-Type')
                    for url in urls:
                        content_db.put(url.encode('utf-8'), payload)

                        if content_type:
                            mimetype_db.put(url.encode('utf-8'), content_type.encode('utf-8'))
                        total_url += 1
                    logging.info(f"Saved record: {uri} [{content_type}]")
                else:
                    logging.debug(f"Payload: {payload}")
            stream.close()

    db.close()

    logging.info(f"Inserted {total_path} paths into the LevelDB, for {total_url} URL variations")
