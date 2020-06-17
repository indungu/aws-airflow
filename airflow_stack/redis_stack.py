from aws_cdk import core
from aws_cdk.aws_ec2 import SecurityGroup, Vpc
from aws_cdk.aws_rds import DatabaseInstance
import aws_cdk.aws_elasticache as elasticache

DB_PORT = 5432
REDIS_PORT = 6379

class RedisStack(core.Stack):

    def __init__(self, scope: core.Construct, id: str, deploy_env: str, config: dict, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)
        self.config = config
        self.vpc = Vpc.from_lookup(self, f"vpc-{deploy_env}", vpc_id=config["vpc_id"])
        db_sg = SecurityGroup.from_security_group_id(self, id=f"RDS-SG-{deploy_env}",
                                                     security_group_id=config["rds_security_group_id"])
        self.postgres_db = DatabaseInstance.from_database_instance_attributes(self, f"airflow-rds-{deploy_env}",
                                                                              instance_identifier=config["rds_instance_id"],
                                                                              instance_endpoint_address=config["rds_endpoint_address"],
                                                                              port=DB_PORT,
                                                                              security_groups=[db_sg])
        self.redis_sg = SecurityGroup(self, f"AirflowRedisSG-{deploy_env}", vpc=self.vpc)
        redis_subnet_group_name = f"AirflowRedisSubnetGrp-{deploy_env}"
        # Ideally we would use private subnets for Redis
        # but current dev VPC has no private subnets
        private_subnet_ids = config["private_subnet_ids"].split(",")
        self.setup_redis(config, deploy_env, redis_subnet_group_name, private_subnet_ids)

    def setup_redis(self, config, deploy_env, redis_subnet_group_name, subnet_ids):
        redis_subnet_group = elasticache.CfnSubnetGroup(self, redis_subnet_group_name,
                                                        subnet_ids=subnet_ids,
                                                        description="Airflow Redis Cache Subnet Group",
                                                        cache_subnet_group_name=redis_subnet_group_name)
        self.redis = elasticache.CfnCacheCluster(self, f"AirflowRedis-{deploy_env}",
                                                 cache_node_type=config["cache_node_type"],
                                                 engine="redis", num_cache_nodes=config["num_cache_nodes"],
                                                 az_mode=config["cache_az_mode"],
                                                 vpc_security_group_ids=[self.redis_security_group.security_group_id],
                                                 cache_subnet_group_name=redis_subnet_group_name)
        self.redis.add_depends_on(redis_subnet_group)

    @property
    def redis_host(self):
        return self.redis.attr_redis_endpoint_address

    @property
    def db_host(self):
        return self.postgres_db.db_instance_endpoint_address

    @property
    def redis_security_group(self):
        return self.redis_sg
