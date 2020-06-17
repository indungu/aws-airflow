import json

import boto3
from invoke import task

from airflow_stack.airflow_stack import get_webserver_service_name, get_webserver_taskdef_family_name, \
    get_scheduler_service_name, get_scheduler_taskdef_family_name, get_worker_service_name, \
    get_worker_taskdef_family_name, get_cluster_name

@task
def destroy(context, env):
    context.run(f'ENV={env} cdk destroy redis-efs-{env} airflow-{env}')

@task
def deploy_airflow(context, env, imageTag=None):
    image_tag_option = ""
    if imageTag:
        image_tag_option = f"--context image_tag={imageTag}"
    role_arn = json.loads(open("conf/{0}/config.json".format(env)).read())["deploy_role_arn"]
    context.run(f"ENV={env} cdk deploy {image_tag_option} --require-approval never --role-arn {role_arn} airflow-{env}")

def stop_service_task(client, deploy_env, cluster_name, task_family):
    task_arn = client.list_tasks(cluster=cluster_name,
                                 family=task_family,
                                 desiredStatus='RUNNING')["taskArns"][0]
    client.stop_task(cluster=get_cluster_name(deploy_env), task=task_arn)

@task
def stop_tasks(context, deploy_env, region_name='us-east-1'):
    client = boto3.client('ecs', region_name=region_name)
    services_and_tasks = [
        (get_webserver_service_name(deploy_env), get_webserver_taskdef_family_name(deploy_env)),
        (get_scheduler_service_name(deploy_env), get_scheduler_taskdef_family_name(deploy_env)),
        (get_worker_service_name(deploy_env), get_worker_taskdef_family_name(deploy_env)),
    ]
    cluster_name = get_cluster_name(deploy_env)
    for service_name, task_family_name in services_and_tasks:
        print(f"Stopping {task_family_name}")
        stop_service_task(client, deploy_env, cluster_name, task_family_name)
