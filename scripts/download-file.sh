s3object=$1
directory=$(awk -F/ '{print $(NF-1)}' <<< "$s3object")

aws s3 cp --profile qfin \
"$s3object" ./s3-downloads/"$directory"/