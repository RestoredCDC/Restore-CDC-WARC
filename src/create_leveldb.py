import gzip
import logging
import os
import re

import plyvel
from warcio.archiveiterator import ArchiveIterator

class WARCLevelDB:
    def __init__(self, dbfolder):
        """
        Initialize the WARCLevelDB class.

        :param dbfolder: Target directory for the WARCLevelDB
        """
        self.dbfolder = dbfolder
        self.db = plyvel.DB(os.path.join(dbfolder, 'cdc_database'), create_if_missing=True)

        # The actual content (could be binary or text)
        self.content_db = self.db.prefixed_db(b"c-")

        # The mimetype of the content, if it is known
        self.mimetype_db = self.db.prefixed_db(b"m-")

        # The date/time of the version we fetched from the IA
        self.timestamp_db = self.db.prefixed_db(b"t-")

        self.total_path = 0
        self.total_url = 0

    def close(self):
        """
        Close the handle for the DB
        """
        self.db.close()

    def process_url(self, subdomain, url_data):
        """
        Extract the WARC file associated with the url, and insert data
        into the different prefixed WARCLevelDB instances

        :param subdomain: The subdomain that we're processing (hivrisk.cdc.gov, nccd.cdc.gov, etc)
        :param url_data: A hashmap with information about a URL in the subdomain
        """
        self.total_path += 1
        filename = url_data['fetched']['file']
        if not filename:
            logging.debug(f"Missing warc file for {url_data}")
            return
        logging.debug(f"Opening file{filename}")
        file_contents = None

        if filename.endswith(".gz"):
            stream = gzip.open(filename, 'rb')
        elif filename.endswith(".warc"):
            stream = open(filename, 'rb')
        else:
            return

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
                    self.content_db.put(url.encode('utf-8'), payload)

                    if content_type:
                        self.mimetype_db.put(url.encode('utf-8'), content_type.encode('utf-8'))
                    self.timestamp_db.put(url.encode('utf-8'), url_data['timestamp'].encode('utf-8'))
                    self.total_url += 1
                logging.info(f"Saved record: {uri} [{content_type}]")
            else:
                logging.debug(f"Payload: {payload}")
        stream.close()
