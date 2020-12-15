import boto3, json, configparser

config = configparser.ConfigParser()
config.read('dwh.cfg')

KEY                = config['AWS']['KEY']
SECRET             = config['AWS']['SECRET']
CLUSTER_TYPE       = config['REDSHIFT']['CLUSTER_TYPE']
CLUSTER_IDENTIFIER = config['REDSHIFT']['IDENTIFIER']
NODE_TYPE          = config['REDSHIFT']['NODE_TYPE']
NUM_NODES          = config['REDSHIFT']['NUM_NODES']
IAM_ROLE           = config['IAM_ROLE']['ARN']
DB_NAME            = config['CLUSTER']['DB_NAME']
DB_USER            = config['CLUSTER']['DB_USER']
DB_PASSWORD        = config['CLUSTER']['DB_PASSWORD']
DB_PORT            = config['CLUSTER']['DB_PORT']

# create redshift cluster
redshift = boto3.client(
    'redshift',
    region_name           = 'us-west-2',
    aws_access_key_id     = KEY,
    aws_secret_access_key = SECRET
)

try:
    redshift.create_cluster(
        ClusterType        = CLUSTER_TYPE,
        NodeType           = NODE_TYPE,
        NumberOfNodes      = int(NUM_NODES),
        DBName             = DB_NAME,
        ClusterIdentifier  = CLUSTER_IDENTIFIER,
        MasterUsername     = DB_USER,
        MasterUserPassword = DB_PASSWORD,
        IamRoles           = [IAM_ROLE]
    )
except Exception as e:
    print(e)

# retrieve cluster endpoint
#clusterDetails = redshift.describe_clusters(
#    ClusterIdentifier = CLUSTER_IDENTIFIER
#)['Clusters'][0]
#
#redshift_host = clusterDetails['Endpoint']['Address']

# add TCP port access to redshift cluster
ec2 = boto3.resource(
    'ec2',
    region_name='ap-northeast-2',
    aws_access_key_id=KEY,
    aws_secret_access_key=SECRET,
)

vpc = ec2.Vpc(id=clusterDetails['VpcId'])
defaultSg = list(vpc.security_groups.all())[0]

defaultSg.authorize_ingress(
    GroupName=defaultSg.group_name,
    CidrIp='0.0.0.0/0',
    IpProtocol='TCP',
    FromPort=int(DB_PORT),
    ToPort=int(DB_PORT)
)

