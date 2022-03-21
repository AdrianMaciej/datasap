# Spark with Localstack

## Requirements
1. awscli
2. docker with docker-compose

## Running environment
```
docker-compose up
```
## Localstack setup
```
# export dummy ACCESS and SECRET
export AWS_ACCESS_KEY_ID=FAKE
export AWS_SECRET_ACCESS_KEY=FAKE

# create bucket
aws --endpoint-url="http://localhost:4572" s3 mb s3://somebucketname

# sync local s3 folder with localstack s3
aws --endpoint-url="http://localhost:4572" s3 sync s3/somebucketname s3://somebucketname
```

## Running script
1. Find id of docker running master via ```docker ps```
2. Bash into container via ```docker exec -it <container_id> /bin/bash```
3. Inside container run script via ```./bin/spark-submit --master spark://0.0.0.0:7077 /work/ETL_script.py 4 s3a://somebucketname/tpcds-filtered s3a://somebucketname/tpcds-dwh```

## Check results
By running ```aws --endpoint-url="http://localhost:4572" s3 ls s3://somebucketname``` check if it contains ```tpcds-dwh``` directory.
You can sync the result files to local disk by running ```aws --endpoint-url="http://localhost:4572" s3 sync s3://somebucketname/tpcds-dwh s3/somebucketname/tpcds-dwh```