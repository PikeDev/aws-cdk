import * as s3 from "@aws-cdk/aws-s3";
import { FileFormat, RowFormat  } from "./props";

/**
 * Options for a new Athena database
 */
export interface DatabaseOptions {

    /**
     * The name of the database.
     */
    readonly name: string;

    /**
     * The tables in this database
     *
     * @default - None
     */
    readonly tables?: TableOptions[];
  }

/**
 * Options for a new Athena table
 */
export interface TableOptions {
    /**
     * The name of the table.
     */
    readonly name: string;

    /**
     * The columns of this table
     */
    readonly columns: Array<[string, string]>;

    /**
     * Specifies the row format of the table and its underlying source data if applicable
     */
    readonly rowFormat: RowFormat;

    /**
     * Specifies the file format for table data
     */
    readonly fileFormat: FileFormat;

    /**
     * The location in Amazon S3 where this table's data resides
     */
    readonly location: s3.IBucket;

    /**
     * The key within the selected `location` bucket
     *
     * @default Root of the bucket
     */
    readonly locationKey?: string;

    /**
     * Whether the data in the `location` bucket is encrypted
     *
     * @default False
     */
    readonly encryptedData?: boolean;
  }