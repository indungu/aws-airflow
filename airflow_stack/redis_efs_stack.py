from aws_cdk import core, aws_ec2, aws_efs
from aws_cdk.aws_ec2 import SecurityGroup, InstanceType, InstanceClass, InstanceSize, AmazonLinuxGeneration, \
    AmazonLinuxEdition, AmazonLinuxStorage, SubnetType, SubnetSelection, MachineImage, Port, Protocol, Vpc
from aws_cdk.aws_efs import PerformanceMode, ThroughputMode
from aws_cdk.aws_rds import DatabaseInstance
import aws_cdk.aws_elasticache as elasticache

DB_PORT = 5432
REDIS_PORT = 6379
MOUNT_POINT = "/mnt/efs"

class RedisEfsStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        vpc = Vpc.from_lookup(self, f"vpc-{deploy_env}", vpc_id=config["vpc_id"])
        db_sg = SecurityGroup.from_security_group_id(self, id=f"RDS-SG-{deploy_env}",
                                                     security_group_id=config["rds_security_group_id"])
        self.postgres_db = DatabaseInstance.from_database_instance_attributes(self, f"airflow-rds-{deploy_env}",
                                                                              instance_identifier=config["rds_instance_id"],
                                                                              instance_endpoint_address=config["rds_endpoint_address"],
                                                                              port=DB_PORT,
                                                                              security_groups=[db_sg])
        self.redis_sg = SecurityGroup(self, f"AirflowRedisSG-{deploy_env}", vpc=vpc)
        redis_subnet_group_name = f"AirflowRedisSubnetGrp-{deploy_env}"
        # Ideally we would use private subnets for Redis
        # but current dev VPC has no private subnets
        subnet_ids = [s.subnet_id for s in vpc.private_subnets] if config.get("use_private_subnets", True) else \
            [s.subnet_id for s in vpc.public_subnets]
        redis_subnet_group = elasticache.CfnSubnetGroup(self, redis_subnet_group_name,
                                                        subnet_ids= subnet_ids,
                                                        description="Airflow Redis Cache Subnet Group",
                                                        cache_subnet_group_name=redis_subnet_group_name)
        self.redis = elasticache.CfnCacheCluster(self, f"AirflowRedis-{deploy_env}",
                                            cache_node_type=config["cache_node_type"],
                                            engine="redis", num_cache_nodes=config["num_cache_nodes"],
                                            az_mode=config["cache_az_mode"],
                                            vpc_security_group_ids=[self.redis_security_group.security_group_id],
                                            cache_subnet_group_name=redis_subnet_group_name)
        self.redis.add_depends_on(redis_subnet_group)
        redis_port_info = Port(protocol=Protocol.TCP, string_representation="allow to redis",
                               from_port=REDIS_PORT, to_port=REDIS_PORT)
        file_system_name = f'AirflowEFS-{deploy_env}'
        # default to private subnets unless VPC does not provide one
        efs_subnet_type = SubnetType.PRIVATE if config.get("use_private_subnets", True) else SubnetType.PUBLIC
        self.efs_file_system = aws_efs.FileSystem(self, file_system_name, file_system_name=file_system_name,
                                                  vpc=vpc, encrypted=False, performance_mode=PerformanceMode.GENERAL_PURPOSE,
                                                  throughput_mode=ThroughputMode.BURSTING,
                                                  vpc_subnets=SubnetSelection(subnet_type=efs_subnet_type))
        self.bastion = self.setup_bastion_access(self.postgres_db, deploy_env, self.redis_sg, vpc, redis_port_info)
        self.setup_efs_volume()

    def setup_efs_volume(self):
        self.efs_file_system.connections.allow_default_port_from(self.bastion)
        self.bastion.add_user_data("yum check-update -y",
                              "yum upgrade -y",
                              "yum install -y amazon-efs-utils",
                              "yum install -y nfs-utils",
                              "file_system_id_1=" + self.efs_file_system.file_system_id,
                              "efs_mount_point_1="+self.mount_point,
                              "mkdir -p \"${efs_mount_point_1}\"",
                              "test -f \"/sbin/mount.efs\" && echo \"${file_system_id_1}:/ ${efs_mount_point_1} efs defaults,_netdev\" >> /etc/fstab || " +
                              "echo \"${file_system_id_1}.efs." + self.region + ".amazonaws.com:/ ${efs_mount_point_1} nfs4 nfsvers=4.1,rsize=1048576,wsize=1048576,hard,timeo=600,retrans=2,noresvport,_netdev 0 0\" >> /etc/fstab",
                              "mount -a -t efs,nfs4 defaults")

    def setup_bastion_access(self, postgres_db, deploy_env, redis_sg, vpc, redis_port_info):
        self.bastion = aws_ec2.Instance(self, f"AirflowBastion-{deploy_env}", vpc=vpc,
                                   instance_type=InstanceType.of(InstanceClass.BURSTABLE2, InstanceSize.MICRO),
                                   machine_image=MachineImage.latest_amazon_linux(generation=AmazonLinuxGeneration.AMAZON_LINUX,
                                                                                edition=AmazonLinuxEdition.STANDARD,
                                                                                storage=AmazonLinuxStorage.GENERAL_PURPOSE),
                                   vpc_subnets=SubnetSelection(subnet_type=SubnetType.PUBLIC),
                                   key_name="airflow")
        # self.bastion.user_data.add_commands("sudo yum check-update -y", "sudo yum upgrade -y",
        #                                         "sudo yum install postgresql-devel python-devel gcc",
        #                                         "virtualenv env && source env/bin/activate && pip install pgcli==1.11.0")
        ssh_port_info = Port(protocol=Protocol.TCP, string_representation="allow ssh",
                             from_port=22, to_port=22)
        # As an alternative to providing a keyname we can use [EC2 Instance Connect]
        # https://aws.amazon.com/blogs/infrastructure-and-automation/securing-your-bastion-hosts-with-amazon-ec2-instance-connect/
        # with the command `aws ec2-instance-connect send-ssh-public-key` to provide your SSH public key.
        self.bastion.connections.allow_from_any_ipv4(ssh_port_info)
        self.bastion.connections.security_groups[0].connections.allow_to_default_port(postgres_db, 'allow PG')
        self.bastion.connections.security_groups[0].connections.allow_to(redis_sg, redis_port_info, 'allow Redis')
        return self.bastion

    @property
    def redis_host(self):
        return self.redis.attr_redis_endpoint_address

    @property
    def db_host(self):
        return self.postgres_db.db_instance_endpoint_address

    @property
    def redis_security_group(self):
        return self.redis_sg

    @property
    def efs_file_system_id(self):
        return self.efs_file_system.file_system_id

    @property
    def mount_point(self):
        return MOUNT_POINT