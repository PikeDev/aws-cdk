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

/**
 * Specifies the row format of the table and its underlying source data if applicable.
 */
export class RowFormat {
  /**
   * Create a custom row format by specifying the delimiters to be used
   *
   * @param fieldDelimiter The field delimiter. Default: ,
   * @param escapeCharacter The field escape character. Default: None
   * @param lineTerminator The line terminator. Default: None
   * @param mapKeyTerminator The map key terminator: Default: None
   */
  public static delimiters(fieldDelimiter?: string, escapeCharacter?: string, lineTerminator?: string, mapKeyTerminator?: string): RowFormat {
    let statement: string = `DELIMITED FIELDS TERMINATED BY '${fieldDelimiter ? fieldDelimiter : ','}'`;
    if (escapeCharacter) {
      statement += ` ESCAPED BY '${RowFormat.escapeSpecialCharacters(escapeCharacter)}'`;
    }
    if (mapKeyTerminator) {
      statement += `\nMAP KEYS TERMINATED BY '${RowFormat.escapeSpecialCharacters(mapKeyTerminator)}'`;
    }
    if (lineTerminator) {
      statement += `\nLINES TERMINATED BY '${RowFormat.escapeSpecialCharacters(lineTerminator)}'`;
    }
    return new RowFormat(statement);
  }

  /**
   * Create a custom row format by specifying the SerDe to be used
   *
   * @param serdeName Indicates the SerDe to use
   * @param serdeProperties Allows you to provide one or more custom properties allowed by the SerDe
   */
  public static serde(serdeName: string, serdeProperties?: Array<[string, string]>): RowFormat {
    let statement: string = `SERDE ${serdeName}`;
    if (serdeProperties && serdeProperties.length > 0) {
      statement += ` WITH SERDEPROPERTIES (${serdeProperties.map(serdeProperty => {
        return `'${serdeProperty[0] = '${serdeProperty[1]}'}'`;
      }).join(',')})`;
    }
    return new RowFormat(statement);
  }

  private static escapeSpecialCharacters(character: string): string {
    const specialCharacters: string[] = [';', '\\', '\''];
    if (specialCharacters.find(x => x === character)) {
      return `\\${character}`;
    }
    return character;
  }

  /**
   * The statement to be used in the table creation query.
   */
  public readonly statement: string;

  private constructor(statement: string) {
    this.statement = statement;
  }
}

/**
 * Specifies the file format for table data
 */
export class FileFormat {
  /**
   * SEQUENCEFILE
   */
  public static readonly SEQUENCEFILE  = new FileFormat('SEQUENCEFILE');

  /**
   * TEXTFILE
   */
  public static readonly TEXTFILE  = new FileFormat('TEXTFILE');

  /**
   * RCFILE
   */
  public static readonly RCFILE  = new FileFormat('RCFILE');

  /**
   * ORC
   */
  public static readonly ORC  = new FileFormat('ORC');

  /**
   * PARQUET
   */
  public static readonly PARQUET  = new FileFormat('PARQUET');

  /**
   * AVRO
   */
  public static readonly AVRO  = new FileFormat('AVRO');

  /**
   * Create a custom file format by specifying the input and output format class names
   * Will produce the following statement:
   * INPUTFORMAT `inputFormatClassName` OUTPUTFORMAT `outputFormatClassName`
   *
   * @param inputFormatClassName Input format class name
   * @param outputFormatClassName Output format class name
   */
  public static fromClassNames(inputFormatClassName: string, outputFormatClassName: string): FileFormat {
    return new FileFormat(`INPUTFORMAT '${inputFormatClassName}' OUTPUTFORMAT '${outputFormatClassName}'`);
  }

  /**
   * The statement to be used in the table creation query.
   */
  public readonly statement: string;

  private constructor(statement: string) {
    this.statement = statement;
  }
}