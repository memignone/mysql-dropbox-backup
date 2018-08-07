import logging
import os
import sys
from datetime import datetime
from subprocess import Popen, PIPE

import dropbox

logger = logging.getLogger(__name__)

# Database info:
DATABASE_HOST = os.environ['DATABASE_HOST']
DATABASE_NAME = os.environ['DATABASE_NAME']
DATABASE_PWD = os.environ['DATABASE_PWD']
DATABASE_USER = os.environ['DATABASE_USER']
# Dropbox account info:
DROPBOX_TOKEN = os.environ['DROPBOX_TOKEN']
DROPBOX_FOLDER = '/'
# Local directory to work in:
TEMP_FOLDER = '/tmp/elclub_backup/'


def run():
    # Create temp dir to store db dumps
    os.makedirs(TEMP_FOLDER, exist_ok=True)
    # Build dump's filename
    dump_fn = f'dump-{datetime.now().isoformat()}.sql.gz'
    # Build dump's file path
    dump_path = os.path.join(TEMP_FOLDER, dump_fn)
    # Dump DB data and compress the result
    logger.info('Dumping MySQL data...')
    mysql_dump_process = Popen(f'mysqldump -h{DATABASE_HOST} -u{DATABASE_USER} -p{DATABASE_PWD}'
                               f' --databases {DATABASE_NAME} --quick --single-transaction | gzip > {dump_path}',
                               shell=True, stderr=PIPE, stdout=PIPE)
    if mysql_dump_process.wait() != 0:
        sys.exit(f'There were errors while dumping db.\n{mysql_dump_process.communicate()}')
    logger.info('MySQL data dump done!')
    # Connect to Dropbox
    dbx = dropbox.Dropbox(DROPBOX_TOKEN)
    # Upload the file
    with open(dump_path, 'rb') as source:
        logger.info('Uploading backup file to Dropbox...')
        # Build dropbox destination path
        dest_path = os.path.join(DROPBOX_FOLDER, dump_fn)
        try:
            dbx.files_upload(source.read(), dest_path, mode=dropbox.files.WriteMode.add)
        except dropbox.exceptions.ApiError as err:
            # This checks for the specific error where a user doesn't have enough
            # Dropbox space quota to upload this file.
            if err.error.is_path() and err.error.get_path().reason.is_insufficient_space():
                sys.exit("ERROR: Cannot back up; insufficient space.")
            elif err.user_message_text:
                logger.error(err.user_message_text)
                sys.exit()
            else:
                logger.error(err)
                sys.exit()


if __name__ == '__main__':
    run()
