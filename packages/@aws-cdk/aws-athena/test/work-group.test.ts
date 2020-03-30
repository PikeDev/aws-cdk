import { expect, haveResource } from '@aws-cdk/assert';
import * as kms from '@aws-cdk/aws-kms';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { FileFormat, ResultEncryptionOption, RowFormat, WorkGroup } from '../lib';

test('Create a workgroup', () => {
  // GIVEN
  const stack = new cdk.Stack();
  const bucket = new s3.Bucket(stack, "Bucket");

  // WHEN
  new WorkGroup(stack, 'WorkGroup', {
    name: 'hello',
    description: 'world',
    enabled: true,
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
    enforceWorkGroupConfiguration: true,
    publishCloudWatchMetricsEnabled: true,
    requesterPaysEnabled: true,
    bytesScannedCutoffPerQuery: 1000,
    resultEncryptionOption: ResultEncryptionOption.SSE_S3
  });

  // THEN
  expect(stack).to(haveResource("AWS::Athena::WorkGroup", {
    Name: "hello",
    Description: "world",
    RecursiveDeleteOption: true,
    State: "ENABLED",
    WorkGroupConfiguration: {
      BytesScannedCutoffPerQuery: 1000,
      EnforceWorkGroupConfiguration: true,
      PublishCloudWatchMetricsEnabled: true,
      RequesterPaysEnabled: true,
      ResultConfiguration: {
        EncryptionConfiguration: {
          EncryptionOption: "SSE_S3"
        },
        OutputLocation: {
          "Fn::Join": [
            "",
            [
              "s3://",
              {
                Ref: "Bucket83908E77"
              },
              "/"
            ]
          ]
        }
      }
    }
  }));
});

test('Create a workgroup with result encryption key ', () => {
  // GIVEN
  const stack = new cdk.Stack();
  const bucket = new s3.Bucket(stack, "Bucket");
  const key = new kms.Key(stack, 'Key');

  // WHEN
  new WorkGroup(stack, 'WorkGroup', {
    name: 'hello',
    description: 'world',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
    resultEncryptionOption: ResultEncryptionOption.SSE_KMS,
    resultEncryptionKmsKey: key
  });

  // THEN
  expect(stack).to(haveResource("AWS::Athena::WorkGroup", {
    Name: "hello",
    Description: "world",
    RecursiveDeleteOption: true,
    State: "ENABLED",
    WorkGroupConfiguration: {
      ResultConfiguration: {
        EncryptionConfiguration: {
          EncryptionOption: "SSE_KMS",
          KmsKey: {
            "Fn::GetAtt": [
              "Key961B73FD",
              "Arn"
            ]
          }
        },
        OutputLocation: {
          "Fn::Join": [
            "",
            [
              "s3://",
              {
                Ref: "Bucket83908E77"
              },
              "/"
            ]
          ]
        }
      }
    }
  }));
});

test('Create a workgroup with database', () => {
  // GIVEN
  const stack = new cdk.Stack();
  const bucket = new s3.Bucket(stack, "Bucket");

  // WHEN
  new WorkGroup(stack, 'WorkGroup', {
    name: 'hello',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
    databases: [{
      name: 'hello'
    }]
  });
  // THEN
  expect(stack).to(haveResource("Custom::AthenaDatabase", {
    CreateQueryString: "CREATE DATABASE IF NOT EXISTS hello",
    UpdateQueryString: "CREATE DATABASE IF NOT EXISTS hello",
    DeleteQueryString: "DROP DATABASE IF EXISTS hello CASCADE"
  }));
});

test('Create a workgroup with database and table', () => {
  // GIVEN
  const stack = new cdk.Stack();
  const bucket = new s3.Bucket(stack, "Bucket");

  // WHEN
  new WorkGroup(stack, 'WorkGroup', {
    name: 'hello',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
    databases: [{
      name: 'hello',
      tables: [{
        name: 'world',
        location: bucket,
        columns: [
            ['ColumnA', 'string'],
            ['ColumnB', 'int']
        ],
        rowFormat: RowFormat.delimiters(';'),
        fileFormat: FileFormat.TEXTFILE
      }]
    }]
  });

  // THEN
  expect(stack).to(haveResource("Custom::AthenaDatabase", {
    CreateQueryString: "CREATE DATABASE IF NOT EXISTS hello",
    UpdateQueryString: "CREATE DATABASE IF NOT EXISTS hello",
    DeleteQueryString: "DROP DATABASE IF EXISTS hello CASCADE"
  }));

  expect(stack).to(haveResource("Custom::AthenaTable", {
    CreateQueryString: {
      "Fn::Join": [
        "",
        [
          "\n      CREATE EXTERNAL TABLE IF NOT EXISTS hello.world (ColumnA string,ColumnB int)\n      ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'\n      STORED AS TEXTFILE\n      LOCATION 's3://",
          {
            Ref: "Bucket83908E77"
          },
          "/'\n      TBLPROPERTIES ('has_encrypted_data' = 'false')"
        ]
      ]
    },
    UpdateQueryString: {
      "Fn::Join": [
        "",
        [
          "\n      CREATE EXTERNAL TABLE IF NOT EXISTS hello.world (ColumnA string,ColumnB int)\n      ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'\n      STORED AS TEXTFILE\n      LOCATION 's3://",
          {
            Ref: "Bucket83908E77"
          },
          "/'\n      TBLPROPERTIES ('has_encrypted_data' = 'false')"
        ]
      ]
    },
    DeleteQueryString: "DROP TABLE hello.world",
    WorkGroup: "hello"
  }));
});

test('Create a table with custom SerDe', () => {
  // GIVEN
  const stack = new cdk.Stack();
  const bucket = new s3.Bucket(stack, "Bucket");

  // WHEN
  new WorkGroup(stack, 'WorkGroup', {
    name: 'hello',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
    databases: [{
      name: 'hello',
      tables: [{
        name: 'world',
        location: bucket,
        columns: [
            ['ColumnA', 'string'],
            ['ColumnB', 'int']
        ],
        rowFormat: RowFormat.serde(
          'org.apache.hadoop.hive.serde2.RegexSerDe', [
            ['input.regex', '^(?!#)([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+([^ ]+)\\s+[^\(]+[\(]([^\;]+).*\%20([^\/]+)[\/](.*)$']
          ]
        ),
        fileFormat: FileFormat.fromClassNames(
          'org.apache.hadoop.mapred.TextInputFormat',
          'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        )
      }]
    }]
  });

  // THEN
  expect(stack).to(haveResource("Custom::AthenaTable", {
    CreateQueryString: {
      "Fn::Join": [
        "",
        [
          "\n      CREATE EXTERNAL TABLE IF NOT EXISTS hello.world (ColumnA string,ColumnB int)\n      ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe' WITH SERDEPROPERTIES (\"input.regex\" = \"^(?!#)([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+([^ ]+)\\\\s+[^(]+[(]([^\\;]+).*%20([^/]+)[/](.*)$\")\n      STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'\n      LOCATION 's3://",
          {
            Ref: "Bucket83908E77"
          },
          "/'\n      TBLPROPERTIES ('has_encrypted_data' = 'false')"
        ]
      ]
    }
  }));
});