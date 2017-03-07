#!/bin/bash
# nothing here for now
./download.sh $1
source ~/demo/bin/activate
python cms_sqlite_loader.py $1
./archive_and_upload.sh $1
