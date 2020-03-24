import { expect, haveResource } from '@aws-cdk/assert';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { ResultEncryptionOption, WorkGroup } from '../lib';

test('Create a basic workgroup', () => {
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

test('Create a database', () => {
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
