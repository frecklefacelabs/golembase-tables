import pkg, { AST } from 'node-sql-parser';
const { Parser } = pkg;
import * as util from 'util';

// The main function that dispatches to the correct parser
function parseSql(sqlString: string): Record<string, any> | null {
	const parser = new Parser();
	let ast = parser.astify(sqlString);

	// Handle both single AST object and array of ASTs (for multi-statement SQL)
	// As requested, we'll just process the first statement if it's an array.
	if (Array.isArray(ast)) {
		ast = ast[0];
	}

	if (!ast) return null;

	switch (ast.type) {
		case 'create':
			return parseCreateTable(ast);
		case 'select':
			return parseSelect(ast);
		case 'insert':
			return parseInsert(ast); // ✨ New case for INSERT
		default:
			throw new Error(`Unsupported SQL statement type: ${ast.type}`);
	}
}

// -----------------------------------------------------------------------------
// CREATE TABLE PARSER
// -----------------------------------------------------------------------------
function parseCreateTable(ast: AST): Record<string, any> | null {
	// This logic remains the same as before.
	if ((ast as any).keyword !== 'table') return null;
	const typeMap: Record<string, string> = {
		'INTEGER': 'number', 'TEXT': 'string', 'REAL': 'number',
	};
	const result: { [key: string]: any } = {
		tableName: (ast as any).table[0].table,
	};
	const indexColumns: string[] = [];
	for (const def of (ast as any).create_definitions) {
		if (def.resource === 'column') {
			const fieldName: string = def.column.column;
			if (fieldName.toLowerCase() === 'tablename' || fieldName.toLowerCase() === 'indexes') {
				throw new Error(`Column name '${fieldName}' is a reserved word and cannot be used.`);
			}
			result[fieldName] = typeMap[def.definition.dataType] || 'unknown';
		}
		else if (def.constraint_type === 'FOREIGN KEY') {
			const localColumn: string = def.definition[0].column;
			if (result[localColumn]) {
				const refTable: string = def.reference_definition.table[0].table;
				const refColumn: string = def.reference_definition.definition[0].column;
				let fkString: string = `|FK:${refTable}:${refColumn}`;
				const constraintName: string | null = def.constraint;
				if (constraintName && constraintName.includes('__view_as__')) {
					const viewColumn = constraintName.split('__view_as__')[1];
					if (viewColumn) fkString += `:${viewColumn}`;
				}
				result[localColumn] += fkString;
			}
		}
		else if (def.resource === 'index') {
			const indexedColumn: string = def.definition[0].column;
			indexColumns.push(indexedColumn);
		}
	}
	if (indexColumns.length > 0) result.indexes = indexColumns.join(',');
	return result;
}

// -----------------------------------------------------------------------------
// SELECT PARSER
// -----------------------------------------------------------------------------
function parseSelect(ast: AST): Record<string, any> {
	// This logic remains the same as before.
	const buildWhereString = (node: any): string => {
		if (!node) return '';
		if (node.type === 'double_quote_string' || node.type === 'single_quote_string') return `"${node.value}"`;
		if (node.type === 'number') return node.value.toString();
		if (node.type === 'column_ref') return node.column;
		if (node.type === 'binary_expr') {
			const operatorMap: Record<string, string> = { 'AND': '&&', 'OR': '||', '=': '=' };
			const left = buildWhereString(node.left);
			const right = buildWhereString(node.right);
			const operator = operatorMap[node.operator] || node.operator;
			const expr = `${left} ${operator} ${right}`;
			return node.parentheses ? `(${expr})` : expr;
		}
		return '';
	};
	const selectedColumns = (ast as any).columns.map((col: any) => col.expr.column).join(',');
	const tableName = (ast as any).from[0].table;
	const typeClause = `type = "${tableName}"`;
	const mainWhereClause = buildWhereString((ast as any).where);
	return { select: selectedColumns, where: `${typeClause} && ${mainWhereClause}` };
}

// -----------------------------------------------------------------------------
// INSERT PARSER (New!)
// -----------------------------------------------------------------------------
function parseInsert(ast: AST): Record<string, any> {
	const insertAst = ast as any; // Cast to 'any' for easier property access
	const result: { [key: string]: any } = {};

	// Set the 'type' property from the table name
	result.type = insertAst.table[0].table;

	const columns: string[] = insertAst.columns;
	// The values are in a nested structure
	const values: any[] = insertAst.values[0].value;

	if (columns.length !== values.length) {
		throw new Error('Insert statement has a mismatch between columns and values.');
	}

	// Map each column to its corresponding value
	columns.forEach((colName, index) => {
		const valueNode = values[index];
		result[colName] = valueNode.value;
	});

	return result;
}

// --- DEMO ---
console.log("--- Parsing INSERT statement ---");
const insertSql = 'insert into users (user_id, username, dept_id, building, phone_number) values (10, "fred", "ACCT", "central", "800-867-5309")';
console.log(JSON.stringify(parseSql(insertSql), null, 2));

console.log("--- Parsing CREATE statements ---");
const createSql = `
  CREATE TABLE users (
      user_id INTEGER,
      username TEXT,
      dept_id INTEGER,
      building TEXT,
      phone_number TEXT,
      CONSTRAINT fk__view_as__department_name 
      FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
      INDEX idx_username (username),
      INDEX idx_dept_id (dept_id)
  )
`;
console.log(JSON.stringify(parseSql(createSql), null, 2));

const createSql2 = `
  CREATE TABLE departments (
      dept_id TEXT,
	  department_name TEXT,
      city TEXT,
      INDEX idx_dept_id (dept_id)
  )
`;
console.log(JSON.stringify(parseSql(createSql2), null, 2));

console.log("\n--- Parsing SELECT statement ---");
const selectSql = 'select username, phone_number from users where username = "fred" and (department = "accounting" or building = "central")';
console.log(JSON.stringify(parseSql(selectSql), null, 2));

