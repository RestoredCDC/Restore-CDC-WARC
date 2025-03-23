import gzip
import logging
import os
import re

import plyvel
from warcio.archiveiterator import ArchiveIterator

def create_db(loc, dbfolder):
    """
    In a given location unpack gzipped files and extract URL and contents into LevelDB
    :param loc: local directory with read and write permissions
    """
    directory = os.fsencode(loc)
    db = plyvel.DB(os.path.join(dbfolder, 'cdc_database'), create_if_missing=True)

    content_db = db.prefixed_db(b"c-")
    mimetype_db = db.prefixed_db(b"m-")
    
    for file in os.listdir(directory):
        filename = os.fsdecode(file)
        full_path = os.path.join(loc, filename)

        logging.debug(f"Opening file{full_path}")
        file_contents = None
        
        if filename.endswith(".gz"):
            stream = gzip.open(full_path, 'rb')
        elif filename.endswith(".warc"):
            stream = open(full_path, 'rb')
        else:
            continue

        for record in ArchiveIterator(stream):
            if record.rec_type == 'response':
                uri = record.rec_headers.get_header('WARC-Target-URI')
                payload = record.content_stream().read()
                if uri and payload:
                    content_db.put(uri.encode('utf-8'), payload)
                    logging.info(f"Saved record: {uri}")

        stream.close()
            
    db.close()       
