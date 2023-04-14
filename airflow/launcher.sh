
eval export AWS_ACCESS_KEY_ID=$(aws configure get aws_access_key_id --profile de_project)
eval export AWS_SECRET_ACCESS_KEY=$(aws configure get aws_secret_access_key --profile de_project)

docker-compose up -d
