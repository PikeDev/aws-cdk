import * as kms from '@aws-cdk/aws-kms';
import * as s3 from '@aws-cdk/aws-s3';
import { Construct, IResource, Resource } from '@aws-cdk/core';
import { CfnWorkGroup } from './athena.generated';
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
   */
  readonly recursiveDeleteOption: boolean;

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

    const workgroup = new CfnWorkGroup(this, 'WorkGroup', {
      name: props.name,
      description: props.description,
      recursiveDeleteOption: props.recursiveDeleteOption,
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

    this.creationTime = workgroup.attrCreationTime;
  }
}