s3bucket=s3://kaiko-delivery-qfinlab-polimi/scripts/

aws s3 cp --profile qfin ./python/plots.py $s3bucket
aws s3 cp --profile qfin ./python/cross-pair-correlation.py $s3bucket
aws s3 cp --profile qfin ./python/intra-pair-correlation.py $s3bucket
aws s3 cp --profile qfin ./python/untar_and_upload.py $s3bucket
aws s3 cp --profile qfin --acl public-read ./python/requirements.txt $s3bucket
aws s3 cp --profile qfin ./emr/install-python-packages.sh $s3bucket
aws s3 cp --profile qfin ./target/scala-2.12/sparktrades_2.12-0.1.jar $s3bucket
aws s3 cp --profile qfin ./emr/order-books-tars.txt $s3bucket