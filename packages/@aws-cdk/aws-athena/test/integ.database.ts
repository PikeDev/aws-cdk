import * as s3 from '@aws-cdk/aws-s3';
import * as cdk from '@aws-cdk/core';
import { FileFormat, RowFormat, WorkGroup } from '../lib';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'aws-cdk-athena-database');

const bucket = new s3.Bucket(stack, 'Bucket');

new WorkGroup(stack, 'WorkGroup', {
    name: 'workgroup',
    recursiveDeleteOption: true,
    resultOutputLocation: bucket,
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
            rowFormat: RowFormat.delimiters(';'),
            fileFormat: FileFormat.fromClassNames(
                'org.apache.hadoop.mapred.TextInputFormat',
                'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
            )
        }]
    }]
});

app.synth();