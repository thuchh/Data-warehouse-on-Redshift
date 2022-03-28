from botocore.exceptions import ClientError
import json
import boto3
import configparser
import time
import psycopg2
    


    
    
def create_iam_role(config):
    
    iam = boto3.client('iam',
                       aws_access_key_id = config.get('AWS','KEY'),                             #
                       aws_secret_access_key = config.get('AWS','SECRET'),
                       region_name = config.get('AWS','REGION')
                      )
    try:
        print('\nCreating a new IAM Role...')
        dwhRole = iam.create_role(
            Path='/',
            RoleName = config.get("DWH", "DWH_IAM_ROLE_NAME"),                                   #
            Description = "Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}
                              }],
                 'Version': '2012-10-17'})
        )    
            
    except iam.exceptions.EntityAlreadyExistsException:
        print('IAM role already exists. Use the existing IAM role')
        role = iam.get_role(RoleName = config.get("DWH", "DWH_IAM_ROLE_NAME"))                   #
        print('IAM Role Arn: {}'.format(role['Role']['Arn']))

        
    try:
        iam.attach_role_policy(
                RoleName = config.get("DWH", "DWH_IAM_ROLE_NAME"),                               #
                PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                )['ResponseMetadata']['HTTPStatusCode']
        print('[>] Successfully create IAM Role')
    except Exception as e:
        print(e)

    roleArn = iam.get_role(RoleName = config.get("DWH", "DWH_IAM_ROLE_NAME"))['Role']['Arn']
    print(roleArn + '\n')
    
    return iam, roleArn
    

    
    

def create_redshift_cluster(config, dwhRole, roleArn):
    ec2 = boto3.resource('ec2',
                         aws_access_key_id = config.get('AWS','KEY'),                             #
                         aws_secret_access_key = config.get('AWS','SECRET'),
                         region_name = config.get('AWS','REGION')
                        )
    
    redshift = boto3.client('redshift',
                            aws_access_key_id = config.get('AWS','KEY'),                             #
                            aws_secret_access_key = config.get('AWS','SECRET'),
                            region_name = config.get('AWS','REGION')
                           )
    
    try:
        response = redshift.create_cluster(        
            #HW 
            ClusterType = config.get("DWH","DWH_CLUSTER_TYPE"),                                      #
            NodeType = config.get("DWH","DWH_NODE_TYPE"),
            NumberOfNodes = int(config.get("DWH","DWH_NUM_NODES")),
            
            
            #Identifiers & Credentials
            DBName = config.get("DWH","DWH_DB"),
            ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER"),
            MasterUsername = config.get("DWH","DWH_DB_USER"),
            MasterUserPassword = config.get("DWH","DWH_DB_PASSWORD"),

            #Roles (for s3 access)
            IamRoles = [roleArn]  
        )
        print('[>] Successfully create Redshift Cluster \n')
    except Exception as e:
        # print('Could not create cluster: ', e)
        pass
        
    return ec2, redshift



        

def get_Cluster_status(config, redshift):
    myClusterProps = redshift.describe_clusters(
            ClusterIdentifier = config.get("DWH","DWH_CLUSTER_IDENTIFIER")                              #
            )['Clusters'][0]
    cluster_status = myClusterProps['ClusterStatus']

    print(' CLuster status: ',myClusterProps['ClusterStatus'])
    return myClusterProps
    


    
def check_Cluster_status(config, redshift):
    print('Checking Cluster status...')
    tries = 0
    start_time = time.time()
    while True:
        tries += 1
        print('Try attemps: ', tries)
        myClusterProps = get_Cluster_status(config, redshift)
        if myClusterProps['ClusterStatus'] == 'available':
            break
        time.sleep(15)

    print('[>] Cluster connected in {}s \n'.format(time.time() - start_time))
    return myClusterProps




def take_note(myClusterProps):
    try:
        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
    except Exception as e:
        print(e)
    
    return DWH_ENDPOINT, DWH_ROLE_ARN



def TCP_connector(config, ec2, myClusterProps):
    try:
        vpc = ec2.Vpc(id=myClusterProps['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.get('DWH', 'DWH_PORT')),
            ToPort=int(config.get('DWH', 'DWH_PORT'))
            )
    except Exception as e:
        # print(e)
        pass
    
    print('[>] Connected to TCP port \n')
    return defaultSg



def connect_database(config, DWH_ENDPOINT):
    DWH_DB_USER = config.get('DWH', 'DWH_DB_USER')
    DWH_DB_PASSWORD = config.get('DWH', 'DWH_DB_PASSWORD')
    #DWH_ENDPOINT = config.get('DWH', 'DWH_ENDPOINT')
    DWH_PORT = config.get('DWH', 'DWH_PORT')
    DWH_DB = config.get('DWH', 'DWH_DB')
    
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT, DWH_DB)
    print('\nConnecting to RedShift database...')
    conn = psycopg2.connect(conn_string)
    print(f'[>] Connected to Redshift database: {conn_string} \n')
    
    return conn



def update_config(config_object, DWH_ENDPOINT, DWH_ROLE_ARN):
    config_object['AWS'] = {
                            'KEY': 'AKIAUSXDEJQ2RP6MVNQP',
                            'SECRET': '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',
                            'REGION': 'us-west-2'
                            }
    config_object['DWH'] = {
                            'DWH_CLUSTER_TYPE': 'multi-node',
                            'DWH_NUM_NODES': '4',
                            'DWH_NODE_TYPE': 'dc2.large',
                            'DWH_IAM_ROLE_NAME': 'dwhRole',
                            'DWH_CLUSTER_IDENTIFIER': 'dwhCluster',
                            'DWH_DB': 'dwh',
                            'DWH_DB_USER': 'dwhuser',
                            'DWH_DB_PASSWORD': 'Passw0rd',
                            'DWH_PORT': '5439',
                            'DWH_ENDPOINT': str(DWH_ENDPOINT),
                            'DWH_ROLE_ARN': str(DWH_ROLE_ARN)
                            }
    config_object['S3'] = {
                            'LOG_DATA': 's3://udacity-dend/log_data',
                            'LOG_JSONPATH': 's3://udacity-dend/log_json_path.json',
                            'SONG_DATA': 's3://udacity-dend/song_data'
                            }
    
    with open('dwh.cfg', 'w') as configfile:
        config_object.write(configfile)
        
    print('[>] Config file updated \n')
    return configfile
    

    

    
def delete_redshift(config, redshift, iam):
    print('Deleting Cluster... ')
    ## 1. Delete the Cluster
    redshift.delete_cluster(ClusterIdentifier=config.get("DWH", "DWH_CLUSTER_IDENTIFIER"), SkipFinalClusterSnapshot=True)

    ## 2. Check status of deletion
    myClusterProps = redshift.describe_clusters(ClusterIdentifier=config.get("DWH","DWH_CLUSTER_IDENTIFIER"))['Clusters'][0]

    ## 3. Delete role 
    iam.detach_role_policy(RoleName=config.get("DWH", "dwh_iam_role_name"), 
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=config.get("DWH", 'dwh_iam_role_name'))
    print('[>] Cluster deleted \n')


    
    

def main():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    
    iam, roleArn = create_iam_role(config)
    ec2, redshift = create_redshift_cluster(config, iam, roleArn)
    myClusterProps = check_Cluster_status(config, redshift) #use get_Cluster_status() to get ClusterStatus and return to while loop
    DWH_ENDPOINT, DWH_ROLE_ARN = take_note(myClusterProps)
    TCP_connector(config, ec2, myClusterProps)
    
    conn = connect_database(config, DWH_ENDPOINT)
    config_object = configparser.ConfigParser()
    update_config(config_object, DWH_ENDPOINT, DWH_ROLE_ARN)     
    #delete_redshift(config, redshift, iam)

    
    
    
    
if __name__ == "__main__":
    main()
