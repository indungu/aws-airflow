#!/usr/bin/env python3
import json
import os
import sys

from aws_cdk import core

from airflow_stack.airflow_stack import AirflowStack
from airflow_stack.redis_efs_stack import RedisEfsStack

app = core.App()
deploy_env = os.environ.get("ENV", "dev")
config = json.loads(open("conf/{0}/config.json".format(deploy_env)).read())
us_east_env = core.Environment(account=config["account_id"], region="us-east-1")

redis_efs_stack = RedisEfsStack(app, f"redis-efs-{deploy_env}", deploy_env, config, env=us_east_env)
airflow_stack = AirflowStack(app, f"airflow-{deploy_env}", deploy_env,  redis_efs_stack,
             config, env=us_east_env)
airflow_stack.add_dependency(redis_efs_stack)

app.synth()
