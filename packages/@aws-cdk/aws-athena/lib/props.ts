/**
 * Encryption options to be used for query result encryption
 */
export enum ResultEncryptionOption {
    /**
     * Client-side encryption with KMS-managed keys
     */
    CSE_KMS = 'CSE_KMS',

    /**
     * Server-side encryption with KMS-managed keys
     */
    SSE_KMS = 'SSE_KMS',

    /**
     * Server-side encryption with Amazon S3-managed keys
     */
    SSE_S3 = 'SSE_S3'
}