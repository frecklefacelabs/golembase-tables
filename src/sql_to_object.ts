import pkg from 'node-sql-parser';
const { Parser } = pkg;
// import * as util from 'util';

// The parser instance
const parser = new Parser();

// The SQL string to be parsed
const sqlString: string = `
  CREATE TABLE users (
      user_id INTEGER,
      username TEXT,
      dept_id INTEGER,
      phone_number TEXT,
      CONSTRAINT fk__view_as__department_name 
      FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
      INDEX idx_username (username),
      INDEX idx_dept_id (dept_id)
  )
`;

// The ast object's type is complex, so 'any' is used for simplicity
let ast: any = null;

try {
	ast = parser.astify(sqlString);
}
catch (e) {
	if (e instanceof Error) {
		console.error(e.message);
	} else {
		console.error('Unknown error', e);
	}
}
//console.log(ast);

/**
 * Transforms the AST into a flat object with support for indexes.
 * @param ast The Abstract Syntax Tree object from node-sql-parser.
 * @returns A flat object representing the schema.
 * @throws An error if a reserved column name ('tableName' or 'indexes') is used.
 */
function transformAstToFlatWithIndexes(ast: any): Record<string, any> | null {
  if (ast.type !== 'create' || ast.keyword !== 'table') {
    return null;
  }

  const typeMap: Record<string, string> = {
    'INTEGER': 'number', 'TEXT': 'string', 'REAL': 'number',
  };

  // The result object's type is updated to allow for the 'indexes' array
  const result: {
    tableName: string;
    indexes?: string;
    [key: string]: any;
  } = {
    tableName: ast.table[0].table,
  };
  const indexColumns: string[] = [];

  for (const def of ast.create_definitions) {
	console.log('=============================================================');
	console.log(def);
    // Handle standard column definitions
    if (def.resource === 'column') {
      const fieldName: string = def.column.column;

      // ✨ ADDED: Error check for reserved column names
      if (fieldName.toLowerCase() === 'tablename' || fieldName.toLowerCase() === 'indexes') {
        throw new Error(`Column name '${fieldName}' is a reserved word and cannot be used.`);
      }

      result[fieldName] = typeMap[def.definition.dataType] || 'unknown';
    } 
    // Handle foreign key definitions
    else if (def.constraint_type === 'FOREIGN KEY') {
      const localColumn: string = def.definition[0].column;
      if (result[localColumn]) {
        const refTable: string = def.reference_definition.table[0].table;
        const refColumn: string = def.reference_definition.definition[0].column;
        let fkString: string = `|FK:${refTable}:${refColumn}`;

        const constraintName: string | null = def.constraint;
        if (constraintName && constraintName.includes('__view_as__')) {
          const viewColumn = constraintName.split('__view_as__')[1];
          // UPDATED: Appending the view column with a colon separator
          if (viewColumn) fkString += `:${viewColumn}`;
        }
        
        result[localColumn] += fkString;
      }
    } 
    // ✨ ADDED: Handle inline index definitions
    else if (def.resource === 'index') {
      const indexedColumn: string = def.definition[0].column;
      indexColumns.push(indexedColumn);
    }
  }

  // Add the collected index columns to the final object if any exist
  if (indexColumns.length > 0) {
    result.indexes = indexColumns.join(',');
  }

  return result;
}


try {
  const mySchemaObject = transformAstToFlatWithIndexes(ast);
  console.log(JSON.stringify(mySchemaObject, null, 2));
} catch (e: any) {
  console.error("Schema generation failed:", e.message);
}
