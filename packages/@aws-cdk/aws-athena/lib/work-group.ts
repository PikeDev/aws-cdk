import * as cfn from '@aws-cdk/aws-cloudformation';
import * as iam from '@aws-cdk/aws-iam';
import * as kms from '@aws-cdk/aws-kms';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import { CfnResource, Construct, IResource, Resource, Stack } from '@aws-cdk/core';
import * as cr from '@aws-cdk/custom-resources';
import { CfnWorkGroup } from './athena.generated';
import { DatabaseOptions, TableOptions } from './options';
import { ResultEncryptionOption } from './props';

/**
 * Properties for a new workgroup
 */
export interface WorkGroupProps {
  /**
   * The workgroup name.
   */
  readonly name: string;

  /**
   * The workgroup description.
   *
   * @default None
   */
  readonly description?: string;

  /**
   * The option to delete the workgroup and its contents even if the workgroup contains any named queries.
   *
   * @default True
   */
  readonly recursiveDeleteOption?: boolean;

  /**
   * Whether the workgroup is enabled or not
   *
   * @default True
   */
  readonly enabled?: boolean;

  /**
   * The upper data usage limit (cutoff) for the amount of bytes a single query in a workgroup is allowed to scan.
   *
   * @default None
   */
  readonly bytesScannedCutoffPerQuery?: number;

  /**
   * If set to "true", the settings for the workgroup override client-side settings.
   * If set to "false", client-side settings are used.
   *
   * @see https://docs.aws.amazon.com/athena/latest/ug/workgroups-settings-override.html
   *
   * @default False
   */
  readonly enforceWorkGroupConfiguration?: boolean;

  /**
   * Indicates that the Amazon CloudWatch metrics are enabled for the workgroup.
   *
   * @default False
   */
  readonly publishCloudWatchMetricsEnabled?: boolean;

  /**
   * If set to true, allows members assigned to a workgroup to reference Amazon S3 Requester Pays buckets in queries.
   * If set to false, workgroup members cannot query data from Requester Pays buckets, and queries that retrieve data
   * from Requester Pays buckets cause an error.
   *
   * @see https://docs.aws.amazon.com/AmazonS3/latest/dev/RequesterPaysBuckets.html
   *
   * @default False
   */
  readonly requesterPaysEnabled?: boolean;

  /**
   * Which encryption type to use for query results
   *
   * @default None (Result encryption disabled)
   */
  readonly resultEncryptionOption?: ResultEncryptionOption

  /**
   * The KMS key for query result encryption. If specified `resultEncryptionOption`
   * will be set.
   *
   * @default - default master key.
   */
  readonly resultEncryptionKmsKey?: kms.IKey;

  /**
   * The location in Amazon S3 where your query results are stored.
   */
  readonly resultOutputLocation: s3.IBucket;

  /**
   * The databases in this workgroup
   *
   * @default - None
   */
  readonly databases?: DatabaseOptions[];
}

/**
 * Amazon Athena workgroup. Each workgroup enables you to isolate queries for you
 * or your group from other queries in the same account
 *
 * @see https://docs.aws.amazon.com/athena/latest/APIReference/API_CreateWorkGroup.html
 */
export interface IWorkGroup extends IResource {

}

/**
 * Create an Athena workgroup
 *
 * @resource AWS::Athena::WorkGroup
 */
export class WorkGroup extends Resource implements IWorkGroup {
  /**
   * The time of creation of this workgroup
   * @attribute
   */
  public readonly creationTime: string;

  /**
   * The name of this workgroup
   */
  public readonly name: string;

  constructor(scope: Construct, id: string, props: WorkGroupProps) {
    super(scope, id);

    this.name = props.name;

    const workGroup = new CfnWorkGroup(this, 'WorkGroup', {
      name: props.name,
      description: props.description,
      recursiveDeleteOption: props.recursiveDeleteOption && props.recursiveDeleteOption,
      state: (props.enabled ?? true) ? 'ENABLED' : 'DISABLED',
      workGroupConfiguration: {
        bytesScannedCutoffPerQuery: props.bytesScannedCutoffPerQuery,
        enforceWorkGroupConfiguration: props.enforceWorkGroupConfiguration,
        publishCloudWatchMetricsEnabled: props.publishCloudWatchMetricsEnabled,
        requesterPaysEnabled: props.requesterPaysEnabled,
        resultConfiguration: {
          encryptionConfiguration: props.resultEncryptionOption
            ? {
              encryptionOption: props.resultEncryptionOption,
              kmsKey: props.resultEncryptionKmsKey && props.resultEncryptionKmsKey.keyArn
            }
            : undefined,
          outputLocation: `s3://${props.resultOutputLocation.bucketName}/`
        }
      }
    });

    if (props.databases) {
      props.databases.forEach(databaseOptions => {
        new Database(this, `Database${databaseOptions.name}`, {
          workGroupResource: workGroup,
          workGroupName: this.name,
          options: databaseOptions
        });
      });
    }

    this.creationTime = workGroup.attrCreationTime;
  }
}

/**
 * Properties for a new Athena database
 */
interface DatabaseProps {
  /**
   * The reference to the workgroup this database is in
   *
   * @default - None
   */
  readonly workGroupResource: CfnResource;

  /**
   * The name of the workgroup this database is in
   *
   * @default - None
   */
  readonly workGroupName: string;

  /**
   * The tables in this database
   *
   * @default - None
   */
  readonly options: DatabaseOptions;
}

/**
 * A new Athena database
 */
class Database extends Construct {

  /**
   * The name of this database
   */
  public readonly name: string;

  constructor(scope: Construct, id: string, props: DatabaseProps) {
    super(scope, id);

    this.name = props.options.name;

    const createQueryString = `CREATE DATABASE IF NOT EXISTS ${props.options.name}`;
    const deleteQueryString = `DROP DATABASE IF EXISTS ${props.options.name} CASCADE`;

    const database = new cfn.CustomResource(this, 'Resource', {
      provider: QueryProvider.getOrCreate(this),
      resourceType: 'Custom::AthenaDatabase',
      properties: {
        CreateQueryString: createQueryString,
        UpdateQueryString: createQueryString,
        DeleteQueryString: deleteQueryString,
        WorkGroup: props.workGroupName
      }
    });

    database.addDependsOn(props.workGroupResource);

    if (props.options.tables) {
      props.options.tables.forEach(tableOptions => {
        new Table(this, `Table${tableOptions.name}`, {
          databaseName: this.name,
          workGroupName: props.workGroupName,
          options: tableOptions
        });
      });
    }
  }
}

/**
 * Properties for a new Athena table
 */
interface TableProps {
  /**
   * The name of the database this table belongs to
   */
  readonly databaseName: string;

  /**
   * The name of the workgroup this table is in
   */
  readonly workGroupName: string;

  /**
   * The options for this table
   */
  readonly options: TableOptions;
}

/**
 * A new Athena table
 */
class Table extends Construct {

  /**
   * The name of this table
   */
  public readonly name: string;

  constructor(scope: Construct, id: string, props: TableProps) {
    super(scope, id);

    this.name = props.options.name;

    const columns = props.options.columns.map(x => `${x[0]} ${x[1]}`).join(',');

    const createQueryString = `
      CREATE EXTERNAL TABLE IF NOT EXISTS ${props.databaseName}.${props.options.name} (${columns})
      ROW FORMAT ${props.options.rowFormat.statement}
      STORED AS ${props.options.fileFormat.statement}
      LOCATION 's3://${props.options.location.bucketName}/${props.options.locationKey ? props.options.locationKey + '/' : ''}'
      TBLPROPERTIES ('has_encrypted_data' = '${props.options.encryptedData ? 'true' : 'false'}')`;

    const deleteQueryString = `DROP TABLE ${props.databaseName}.${props.options.name}`;

    new cfn.CustomResource(this, 'Resource', {
      provider: QueryProvider.getOrCreate(this),
      resourceType: 'Custom::AthenaTable',
      properties: {
        CreateQueryString: createQueryString,
        UpdateQueryString: createQueryString,
        DeleteQueryString: deleteQueryString,
        WorkGroup: props.workGroupName
      }
    });
  }
}

class QueryProvider extends Construct {

  /**
   * Returns the singleton provider.
   */
  public static getOrCreate(scope: Construct): cr.Provider {
    const stack = Stack.of(scope);
    const id = 'com.amazonaws.cdk.athena.query-provider';
    const x = stack.node.tryFindChild(id) as QueryProvider || new QueryProvider(stack, id);
    return x.provider;
  }

  private readonly provider: cr.Provider;

  constructor(scope: Construct, id: string) {
    super(scope, id);

    const onEvent = new lambda.Function(this, 'QueryLambda', {
      code: new lambda.InlineCode(handlerCode),
      runtime: lambda.Runtime.PYTHON_3_7,
      handler: 'index.on_event',
      initialPolicy: [
        new iam.PolicyStatement({
          resources: [ '*' ],
          actions: [
            'athena:*'
          ]
        }),
        new iam.PolicyStatement({
          resources: [ '*' ],
          actions: [
            's3:*'
          ]
        }),
        new iam.PolicyStatement({
          resources: [ '*' ],
          actions: [
            'glue:*',
          ]
        })
      ]
    });

    this.provider = new cr.Provider(this, 'QueryProvider', {
      onEventHandler: onEvent,
    });
  }
}

/**
 * Handler code for the query provider function
 * Adding this in code instead of an asset so that `import cfnresponse` can be used
 */
const handlerCode = `
import logging
import cfnresponse
import botocore.session
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = botocore.session.get_session()
client = session.create_client('athena')

def on_event(event, context):
    try:
        physical_id = 'AthenaQuery' + event['RequestId']
        request_type = event['RequestType']

        queryString = ''
        if request_type == 'Create':
            queryString = event['ResourceProperties']['CreateQueryString']
        elif request_type == 'Update':
            queryString = event['ResourceProperties']['UpdateQueryString']
        elif request_type == 'Delete':
            queryString = event['ResourceProperties']['DeleteQueryString']

        print(queryString)

        workGroup = event['ResourceProperties']['WorkGroup']

        query = client.start_query_execution(
            QueryString=queryString,
            WorkGroup=workGroup
        )

        query_id = query['QueryExecutionId']

        sleep_amount = 2
        max_attempts = 20
        num_attempts = 0
        execution_state = 'RUNNING'

        while execution_state == 'RUNNING' or execution_state == 'QUEUED':
          execution = client.get_query_execution(QueryExecutionId=query_id)
          num_attempts += 1

          execution_state = execution['QueryExecution']['Status']['State']
          if execution_state == 'SUCCEEDED':
            print('Query succeeded')
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {}, physical_id)
            break
          elif execution_state == 'CANCELLED' or execution_state == 'FAILED' or num_attempts >= max_attempts:
            print('Query failed or cancelled: ' + execution['QueryExecution']['Status']['StateChangeReason'])
            cfnresponse.send(event, context, cfnresponse.SUCCESS, {}, physical_id)
            break

          time.sleep(sleep_amount)

    except Exception as e:
        logger.exception(e)
        cfnresponse.send(event, context, cfnresponse.FAILED, {}, physical_id)
`;