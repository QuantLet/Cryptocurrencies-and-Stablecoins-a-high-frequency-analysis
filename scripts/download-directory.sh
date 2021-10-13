s3directory=$1
localDirectory=$2
includePattern=$3

aws s3 cp --profile qfin --recursive \
 "$s3directory" \
 "$localDirectory" \
 --exclude "*" --include "$includePattern"