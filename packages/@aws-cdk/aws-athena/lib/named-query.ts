import { Construct, IResource, Resource } from '@aws-cdk/core';
import { CfnNamedQuery } from "./athena.generated";

/**
 * Properties for a new named query
 */
export interface NamedQueryProps {
  /**
   * The database to which the query belongs.
   */
  readonly database: string;

  /**
   * The query description.
   *
   * @default - None
   */
  readonly description?: string;

  /**
   * The query name.
   *
   * @default - None
   */
  readonly name?: string;

  /**
   * The SQL query statements that comprise the query.
   */
  readonly queryString: string;
  }

/**
 * Amazon Athena named query
 */
export interface INamedQuery extends IResource {

}

/**
 * Create an Athena named query
 *
 * @resource AWS::Athena::NamedQuery
 */
export class NamedQuery extends Resource implements INamedQuery {

  constructor(scope: Construct, id: string, props: NamedQueryProps) {
    super(scope, id);

    new CfnNamedQuery(this, 'Resource', {
      database: props.database,
      description: props.description,
      name: props.name,
      queryString: props.queryString
    });
  }
}