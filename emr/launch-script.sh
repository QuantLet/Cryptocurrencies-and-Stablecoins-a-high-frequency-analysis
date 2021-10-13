steps=$1

aws emr create-cluster --profile qfin \
    --release-label emr-6.2.0 \
    --instance-fleets file://./fleet-configuration.json \
    --name stablecoin-cluster \
    --ec2-attributes KeyName=stablecoin,SubnetIds=['subnet-5210675c','subnet-9b2f9dfd','subnet-9bc977c4','subnet-b14409fc','subnet-d8fd3ce9','subnet-e1a21ac0'] \
    --applications Name=Spark \
    --steps file://./"$steps" \
    --log-uri s3://kaiko-delivery-qfinlab-polimi/logs/ \
    --auto-terminate \
    --no-termination-protected \
    --use-default-roles \
    --configurations file://./spark-configuration.json \
    --bootstrap-actions Path="s3://kaiko-delivery-qfinlab-polimi/scripts/install-python-packages.sh" \
    #--managed-scaling-policy file://./policy-configuration.json \
