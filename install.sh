# nothing here for now
source download.sh $1
python cms_sqlite_loader.py $1
source archive_and_upload.sh
