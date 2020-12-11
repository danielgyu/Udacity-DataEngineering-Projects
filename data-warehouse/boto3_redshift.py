import boto3
import json

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

KEY                = config['AWS']['KEY']
SECRET             = config['AWS']['SECRET']
CLUSTER_TYPE       = config['REDSHIFT']['CLUSTER_TYPE']
CLUSTER_IDENTIFIER = config['REDSHIFT']['IDENTIFER']
NODE_TYPE          = config['REDSHIFT']['NODE_TYPE']
NUM_NODES          = config['REDSHIFT']['NUM_NODES']
IAM_ROLE           = config['IAM_ROLE']['ARN']

# create redshift cluster
redshift = boto3.client(
    'redshift',
    region_name           = 'ap-northeast-2',
    aws_access_key_id     = KEY,
    aws_secret_access_key = SECRET
)

redshift.create_cluster(
    ClusterType        = CLUETR_TYPE,
    NodeType           = NODE_TYPE,
    NumberOfNodes      = NUM_NODES,
    DBName             = DB_NAME,
    ClusterIdentifier  = CLUSTER_IDENTIFIER,
    MasterUsername     = DB_USER,
    MasterUserPassword = DB_PASSWORD,
    IamRoles           = IAM_ROLE
)

clusterDetails = redshift.describe_clusters(
    ClusterIdentifier = IDENTIFIER
)['Clusters'][0]

redshift_host = clusterDetails['Endpoint']['Address']

# add TCP port access to redshift cluster
ec2 = boto3.resource(
    'ec2',
    region_name='ap-northeast-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

vpc = ec2.Vpc(id=myClusterProps['VpcId'])
defaultSg = list(vpc.security_groups.all())[0]

defaultSg.authorize_ingress(
    GroupName=defaultSg.group_name,
    CidrIp='0.0.0.0/0',
    IpProtocol='TCP',
    FromPort=int(DB_PORT),
    ToPort=int(DB_PORT)
)

