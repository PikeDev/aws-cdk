import * as cdk from '@aws-cdk/core';
import { NamedQuery } from '../lib';

const app = new cdk.App();
const stack = new cdk.Stack(app, 'aws-cdk-athena-named-query');

new NamedQuery(stack, 'NamedQuery', {
    database: 'hello',
    queryString: 'SELECT * FROM World;'
});

app.synth();