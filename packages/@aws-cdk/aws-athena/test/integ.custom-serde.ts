import * as kms from '@aws-cdk/aws-kms';
import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { FileFormat, ResultEncryptionOption, RowFormat, WorkGroup } from '../lib';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'aws-cdk-athena-table-custom-serde');

const key = new kms.Key(stack, 'Key');

const bucket = new s3.Bucket(stack, 'Bucket', {
    encryptionKey: key
});

new WorkGroup(stack, 'WorkGroup', {
    name: 'workgroup',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
    resultEncryptionOption: ResultEncryptionOption.SSE_KMS,
    resultEncryptionKmsKey: key,
    databases: [{
        name: 'hello',
        tables: [{
            name: 'world',
            location: bucket,
            locationKey: 'data',
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

app.synth();