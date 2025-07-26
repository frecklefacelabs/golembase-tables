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
      FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
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
 * Transforms the AST into a flat object with type and foreign key info.
 * @param ast The Abstract Syntax Tree object from node-sql-parser.
 * @returns A flat object representing the schema, or null if parsing fails.
 */
function transformAstToFlatWithView(ast: any): Record<string, string> | null {
	if (ast.type !== 'create' || ast.keyword !== 'table') {
		return null;
	}

	const typeMap: Record<string, string> = {
		'INTEGER': 'number',
		'TEXT': 'string',
		'REAL': 'number',
	};

	const result: { tableName: string;[key: string]: string } = {
		tableName: ast.table[0].table,
	};

	for (const def of ast.create_definitions) {
		if (def.resource === 'column') {
			const fieldName: string = def.column.column;
			const sqlDataType: string = def.definition.dataType;
			result[fieldName] = typeMap[sqlDataType] || 'unknown';
		}
		// CORRECTED: Use 'FOREIGN KEY' (uppercase) and the new AST paths
		else if (def.constraint_type === 'FOREIGN KEY') {
			// Path changed from def.columns[0].column
			const localColumn: string = def.definition[0].column;

			if (result[localColumn]) {
				const refTable: string = def.reference_definition.table[0].table;
				// Path changed from def.reference_definition.columns[0].column
				const refColumn: string = def.reference_definition.definition[0].column;
				let fkString: string = `|FK:${refTable}:${refColumn}`;

				// ✨ NEW, MORE ROBUST METHOD: Parse the constraint name
				const constraintName: string | null = def.constraint;
				if (constraintName && constraintName.includes('__view_as__')) {
					const viewColumn = constraintName.split('__view_as__')[1];
					if (viewColumn) {
						fkString += `:${viewColumn}`; // Append the view column
					}
				}

				result[localColumn] += fkString;
			}
		}
	}

	return result;
}

const myFlatObject = transformAstToFlatWithView(ast);

console.log(JSON.stringify(myFlatObject, null, 2));
