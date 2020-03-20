import * as s3 from '@aws-cdk/aws-s3';
import * as kms from '@aws-cdk/aws-kms';
import { Construct, IResource, Resource } from '@aws-cdk/core';
import { ResultEncryptionOption } from './props';
import { CfnWorkGroup } from './athena.generated';

export interface WorkGroupProps {
  /**
   * The workgroup name.
   */
  readonly name: string;

  /**
   * The workgroup description.
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

  readonly bytesScannedCutoffPerQuery?: number;

  readonly enforceWorkGroupConfiguration?: boolean;

  readonly publishCloudWatchMetricsEnabled?: boolean;

  readonly requesterPaysEnabled?: boolean;

  /**
   * Which encryption type to use for query results
   *
   * @default Result encryption disabled
   */
  readonly resultEncryptionOption?: ResultEncryptionOption

  /**
   * The KMS key for query result encryption. If specified `resultEncryptionOption`
   * will be set.
   *
   * @default - default master key.
   */
  readonly resultEncryptionKmsKey?: kms.IKey;

  readonly resultOutputLocation: s3.IBucket;
}

export interface IWorkGroup extends IResource {

}

export class WorkGroup extends Resource implements IWorkGroup {
  constructor(scope: Construct, id: string, props: WorkGroupProps) {
    super(scope, id);

    new CfnWorkGroup(this, 'WorkGroup', {
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
  }
}