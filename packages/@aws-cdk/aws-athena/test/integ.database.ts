import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { Database, FileFormat, RowFormat, Table, WorkGroup } from '../lib';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'aws-cdk-athena-database');

const bucket = new s3.Bucket(stack, 'Bucket');

const workGroup = new WorkGroup(stack, 'WorkGroup', {
    name: 'workgroup',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket
});

new Database(stack, 'Database', {
    name: 'hello',
    workGroupName: workGroup.name
});

new Table(stack, 'Table', {
    name: 'world',
    workGroupName: workGroup.name,
    databaseName: 'hello',
    location: bucket,
    locationKey: 'data',
    columns: [
        ['ColumnA', 'string'],
        ['ColumnB', 'int']
    ],
    rowFormat: RowFormat.delimiters(';'),
    fileFormat: FileFormat.fromClassNames(
        'org.apache.hadoop.mapred.TextInputFormat',
        'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
    )
});

app.synth();