Install AWS CDK (https://docs.aws.amazon.com/cdk/latest/guide/getting_started.html)
Install Docker (to build the image)

There are 3 stacks that need to be setup: 
 1. VPC stack
 2. RDS, Redis, EFS file system and a bastion - EC2 instance to allow access to RDS, EFS and Redis 
 as they are in private subnets.
 3. ECS Fargate cluster to host airflow components - web-server, scheduler and worker - 
 in separate containers. The webserver service is fronted with an ALB.

### Steps to deploy
1. Setup or use an existing RDS postgres instance.  Update the config.json file in conf/dev with the 
RDS information vpc id and account id. Setup the database password as a 
secret in the parameter store. 

2. The bastion is setup to allow ssh with key_name="airflow". Please create a EC2 key
pair with that name and save the private key so you are able to ssh to it later on.

3. Setup an AWS ECR repo to push docker images. Update the repo name in conf/dev/config.json file.

4. Setup a virtual environment, activate and install requirements.txt.

5. First deploy the VPC and redis/EFS stacks to setup the redis, bastion and EFS:
```cdk deploy redis-efs-dev``` or run the task to do it: ```invoke deploy-redis-efs dev```

6. Ensure that you can login to the RDS postgres db by ssh'ing to the bastion instance. Find
the public IP address of the bastion from AWS console and login in to it using the key:
```ssh -i '~\.ssh\airflow.pem' ec2-user@ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com```
Once logged in, install pgcli (or other sql client) to verify access to the RDS and 
to create a database called airflow.  The following steps install pgcli:  ```sudo yum install postgresql-devel python-devel gcc```
 ```virtualenv env && source env/bin/activate && pip install pgcli==1.11.0```
 ``` pgcli -h<db>.us-east-1.rds.amazonaws.com postgres airflow```. Once in pgcli then create the airflow database: ```create database airflow```

7. Since we are deploying assets to ECR need to run: ```cdk bootstrap aws://<aws-account-id>/us-east-1```

8. Deploy airflow stack on ECS: ```invoke deploy-airflow dev <efs-file-system-id>```
This will push an updated docker image for Airflow to ECR and then use it to setup containers in ECS for the  
web server, scheduler and worker. It will also setup EFS mount for all the containers. Dags can be added
to this location: ```dags_folder = /mnt/efs/airflow/dags``` Once deployed use the Load balancer URL for the web server to get
the airflow UI.


