mv *.db sample_$1
cd sample_$1
gzip *.db
aws s3 sync 
