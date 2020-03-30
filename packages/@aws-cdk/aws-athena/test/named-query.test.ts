import { expect, haveResource } from '@aws-cdk/assert';
import * as cdk from '@aws-cdk/core';
import { NamedQuery } from '../lib';

test('Create a named query', () => {
  // GIVEN
  const stack = new cdk.Stack();

  // WHEN
  new NamedQuery(stack, 'NamedQuery', {
    name: 'hello',
    description: 'world',
    database: "db",
    queryString: 'SELECT * FROM Foo;'
  });

  // THEN
  expect(stack).to(haveResource("AWS::Athena::NamedQuery", {
    Database: "db",
    QueryString: "SELECT * FROM Foo;",
    Description: "world",
    Name: "hello"
  }));
});