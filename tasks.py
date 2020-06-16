import boto3
from invoke import task

from airflow_stack.airflow_stack import get_webserver_service_name, get_webserver_taskdef_family_name, \
    get_scheduler_service_name, get_scheduler_taskdef_family_name, get_worker_service_name, \
    get_worker_taskdef_family_name, get_cluster_name
from airflow_stack.redis_efs_stack import MOUNT_POINT

@task
def destroy(context, env):
    context.run(f'ENV={env} cdk destroy redis-efs-{env} airflow-{env}')

@task
def deploy_redis_efs(context, env):
    context.run(f"ENV={env} cdk deploy --require-approval never redis-efs-{env}")

@task
def deploy_airflow(context, env, imageTag=None, file_system_id=None):
    image_tag_option = ""
    if imageTag:
        image_tag_option = f"--context image_tag={imageTag}"
    context.run(f"ENV={env} cdk deploy {image_tag_option} --require-approval never airflow-{env}")
    if file_system_id:
        setup_efs(context, env, file_system_id)

@task
def stop_scheduler_service(context, deploy_env, region="us-east-1"):
    client = boto3.client("ecs", region_name=region)
    stop_service_task(client, deploy_env, get_cluster_name(deploy_env),
                      get_scheduler_taskdef_family_name(deploy_env))


def stop_service_task(client, deploy_env, cluster_name, task_family):
    task_arn = client.list_tasks(cluster=cluster_name,
                                 family=task_family,
                                 desiredStatus='RUNNING')["taskArns"][0]
    client.stop_task(cluster=get_cluster_name(deploy_env), task=task_arn)


# cannot map volumes to Fargate task defs yet - so this is done via Boto3 since CDK does not
 # support it yet: https://github.com/aws/containers-roadmap/issues/825
@task
def setup_efs(context, deploy_env, file_system_id, region_name='us-east-1'):
    client = boto3.client('ecs', region_name=region_name)
    services_and_tasks = [
        (get_webserver_service_name(deploy_env), get_webserver_taskdef_family_name(deploy_env)),
        (get_scheduler_service_name(deploy_env), get_scheduler_taskdef_family_name(deploy_env)),
        (get_worker_service_name(deploy_env), get_worker_taskdef_family_name(deploy_env)),
    ]
    cluster_name = get_cluster_name(deploy_env)
    for service_name, task_family_name in services_and_tasks:
        update_service_task_def_with_efs_volume(client, file_system_id, service_name, task_family_name,
                                                cluster_name, MOUNT_POINT)
        # the tasks may not update - so stop the task manually so it is restarted with the new updated defn
        task_arn = wait_for_task_to_be_running(client, cluster_name, task_family_name)
        print(f"Stopping {task_family_name}")
        client.stop_task(cluster=cluster_name, task=task_arn)


def wait_for_task_to_be_running(client, cluster_name, task_family_name):
    time_waited = 0
    while time_waited < 60:
        time_waited += 10
        tasks = client.list_tasks(cluster=cluster_name,
                             family=task_family_name,
                             desiredStatus='RUNNING')["taskArns"]
        if tasks:
            return tasks[0]
    raise Exception(f"Timed out waiting for task {task_family_name} to be running")

def update_service_task_def_with_efs_volume(client, file_system_id, service_name, task_family_name,
                                            cluster_name, container_path):
    print(task_family_name)
    response = client.describe_task_definition(taskDefinition=task_family_name)
    task_def = response["taskDefinition"]
    #print(task_def)
    volumes = [
        {
            'name': "efsvolume",
            'efsVolumeConfiguration': {
                'fileSystemId': file_system_id,
                'transitEncryption': 'DISABLED'
            }
        },
    ]
    mount_points = [{"sourceVolume": "efsvolume", "containerPath": container_path}]
    task_def_arn = task_def["taskDefinitionArn"]
    add_volumes(task_def, volumes, mount_points)
    client.deregister_task_definition(taskDefinition=task_def_arn)
    response = client.register_task_definition(**task_def)
    updated_task_def_arn = response["taskDefinition"]["taskDefinitionArn"]
    client.update_service(
        cluster=cluster_name,
        service=service_name,
        taskDefinition=updated_task_def_arn,
    )


def add_volumes(task_def, volumes, mount_points):
    task_def["volumes"] = volumes
    del task_def["taskDefinitionArn"]
    del task_def["revision"]
    del task_def["status"]
    del task_def["requiresAttributes"]
    compatibilities = task_def["compatibilities"]
    del task_def["compatibilities"]
    task_def["requiresCompatibilities"] = compatibilities
    task_def["containerDefinitions"][0]["mountPoints"] = mount_points

