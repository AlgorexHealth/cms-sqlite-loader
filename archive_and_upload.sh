mv *.db sample_$1
cd sample_$1
gzip *.db
aws s3 cp . s3://algorex-cms-synthetic/sample_$1 --recursive --exclude "*" --include "*.db.gz"
