import * as cfn from '@aws-cdk/aws-cloudformation';
import * as iam from '@aws-cdk/aws-iam';
import * as lambda from '@aws-cdk/aws-lambda';
import * as s3 from '@aws-cdk/aws-s3';
import { Construct, Stack } from '@aws-cdk/core';
import * as cr from '@aws-cdk/custom-resources';
import { FileFormat, RowFormat } from './props';

/**
 * Properties for a new Athena database
 */
export interface DatabaseProps {

  /**
   * The name of the database.
   */
  readonly name: string;

  /**
   * The name of the workgroup this database is in
   *
   * @default None
   */
  readonly workGroupName?: string;
}

/**
 * A new Athena database
 */
export class Database extends Construct {

  /**
   * The name of this database
   */
  public readonly name: string;

  constructor(scope: Construct, id: string, props: DatabaseProps) {
    super(scope, id);

    this.name = props.name;

    const createQueryString = `CREATE DATABASE IF NOT EXISTS ${props.name}`;
    const deleteQueryString = `DROP DATABASE IF EXISTS ${props.name} CASCADE`;

    new cfn.CustomResource(this, 'Resource', {
      provider: QueryProvider.getOrCreate(this),
      resourceType: 'Custom::AthenaDatabase',
      properties: {
        CreateQueryString: createQueryString,
        UpdateQueryString: createQueryString,
        DeleteQueryString: deleteQueryString,
        WorkGroup: props.workGroupName
      }
    });
  }
}

/**
 * Properties for a new Athena table
 */
export interface TableProps {

  /**
   * The name of the table.
   */
  readonly name: string;

  /**
   * The name of the database this table belongs to
   *
   * @default None
   */
  readonly databaseName?: string;

  /**
   * The name of the workgroup this table is in
   *
   * @default None
   */
  readonly workGroupName?: string;

  /**
   * The columns of this table
   */
  readonly columns: Array<[string, string]>;

  /**
   * Specifies the row format of the table and its underlying source data if applicable
   */
  readonly rowFormat: RowFormat;

  /**
   * Specifies the file format for table data
   */
  readonly fileFormat: FileFormat;

  /**
   * The location in Amazon S3 where this table's data resides
   */
  readonly location: s3.IBucket;

  /**
   * The key within the selected `location` bucket
   *
   * @default Root of the bucket
   */
  readonly locationKey?: string;

  /**
   * Whether the data in the `location` bucket is encrypted
   *
   * @default False
   */
  readonly encryptedData?: boolean;
}

/**
 * A new Athena table
 */
export class Table extends Construct {

  /**
   * The name of this table
   */
  public readonly name: string;

  constructor(scope: Construct, id: string, props: TableProps) {
    super(scope, id);

    this.name = props.name;

    const columns = props.columns.map(x => `${x[0]} ${x[1]}`).join(',');

    const createQueryString = `
      CREATE EXTERNAL TABLE IF NOT EXISTS ${props.databaseName}.${props.name} (${columns})
      ROW FORMAT ${props.rowFormat.statement}
      STORED AS ${props.fileFormat.statement}
      LOCATION 's3://${props.location.bucketName}/${props.locationKey ? props.locationKey + '/' : ''}'
      TBLPROPERTIES ('has_encrypted_data' = '${props.encryptedData ? 'true' : 'false'}')`;

    const deleteQueryString = `DROP TABLE ${props.databaseName}.${props.name}`;

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

const handlerCode: string = `
import logging
import cfnresponse
import botocore.session
import time

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = botocore.session.get_session()
client = session.create_client('athena')

def on_event(event, context):
    print(event)
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