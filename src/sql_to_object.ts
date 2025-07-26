import { transformPOJOToAnnotations } from '@freckleface/golembase-js-transformations';
import { GolemBaseCreate, GolemBaseUpdate } from 'golem-base-sdk';
import pkg, { AST } from 'node-sql-parser';
const { Parser } = pkg;
import * as util from 'util';

const encoder = new TextEncoder();
const decoder = new TextDecoder();

/**
 * TODO: Very early on let's add some form of BSON / binary data so they can upload images and docs.
 *       The SQL Parser allows for a type called BINARY. In that we would store the hash of the entity containing the data.
 * Here are the types the parser recognizes for CREATE TABLE:
 * 
 * Expected "#", "--", ".", "/*", "BIGINT", "BINARY", "BIT", "CHAR", "COLLATE", "DATE", "DATETIME", "DECIMAL", "DOUBLE", 
 * "ENUM", "FLOAT", "GEOMETRY", "GEOMETRYCOLLECTION", "INT", "INTEGER", "JSON", "LINESTRING", "LONGTEXT", "MEDIUMINT", 
 * "MEDIUMTEXT", "MULTILINESTRING", "MULTIPOINT", "MULTIPOLYGON", "NUMERIC", "POINT", "POLYGON", "SET", "SMALLINT", 
 * "TIME", "TIMESTAMP", "TINYINT", "TINYTEXT", "VARBINARY", "VARCHAR", "YEAR", "blob", "boolean", "longblob", 
 * "mediumblob", "tinyblob", or [ \t\n\r] but "T" found.
 */

/* CreateTablerSQLToGBCreate
   NEED TO FIGURE OUT: Should this call to the node? I need to know if the indexer already exists,
   and I'm not sure I should make the caller handle first retreiving the index.
   But that would "step up" this from a passive parser to an active player, reading/writing data.
   THOUGHT: Build a "second level" on top of this which handles that part?
   THOUGHT2: Put indexes in the table schema right here
 *
 * This returns several items:
 *   Either a Create or Update for the table
 *   Either a Create or Update for the (new or updated) index.
 *
 * 
 * It's up to the caller to send it across to a node
*/
export const SQLCreateTableToGBCreate = (app:string, createSql: string): GolemBaseCreate => {

	let createTableObj: any = { app: app, ...parseSql(createSql) };

	console.log(createTableObj);
	
	// Create empty index entries, which will get filled in as we add data for this table
	for (let index of createTableObj.indexes?.split(',') || []) {
		createTableObj[`index_${index}`] = '';
	}
	console.log(createTableObj);

	const create: GolemBaseCreate = {
		data: encoder.encode(`${createTableObj.type} ${createTableObj.tablename}`),
		btl: 100,
		...transformPOJOToAnnotations(createTableObj)
	};

	return create;
}

export const SQLInsertToGBCreate = (app: string, insertSQL: string): GolemBaseCreate => {

	let insertObj: any = { app: app, ...parseSql(insertSQL) };
	console.log(insertObj);

	const create: GolemBaseCreate = {
		data: encoder.encode(`${insertObj.type} ${insertObj.tablename}`),
		btl: 100,
		...transformPOJOToAnnotations(insertObj)
	};

	return create;
}

/**
 * Filters an object to include only specified keys, plus a set of mandatory keys.
 *
 * @param select A comma-delimited string of keys to include in the result.
 * @param obj The source object to filter.
 * @returns A new object containing only the selected and mandatory keys.
 */
export const filterObjectBySelect = (select: string, obj: Record<string, any>): Record<string, any> => {
  // Get the list of keys to keep from the 'select' string.
  const keysToKeep = select.split(',');

  // Initialize the new object with the three always-included fields.
  const filteredObj: Record<string, any> = {
    app: obj.app,
    type: obj.type,
    tablename: obj.tablename,
  };

  // Iterate over the keys we want to keep.
  for (const key of keysToKeep) {
    // Check if the source object actually has this property before adding it.
    if (obj.hasOwnProperty(key)) {
      filteredObj[key] = obj[key];
    }
  }

  return filteredObj;
}

// The main function that dispatches to the correct parser
export const parseSql = (sqlString: string): Record<string, any> | null => {
	try {
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
	catch (e) {
		if (e instanceof Error) {
			throw (e.message);
		}
		else {
			throw ('Unknown error');
		}
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
		type: 'table',
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
	const typeClause = `type = "tabledata" && tablename = "${tableName}"`;
	const mainWhereClause = buildWhereString((ast as any).where);
	return { select: selectedColumns, where: `${typeClause}${mainWhereClause===''?'':' && '}${mainWhereClause}` };
}

// -----------------------------------------------------------------------------
// INSERT PARSER
// -----------------------------------------------------------------------------
function parseInsert(ast: AST): Record<string, any> {
	const insertAst = ast as any; // Cast to 'any' for easier property access
	const result: { [key: string]: any } = {};

	// Set the type to tabledata
	result.type = 'tabledata';

	// Set the 'tablename' property from the table name
	result.tablename = insertAst.table[0].table;

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
// console.log("--- Parsing INSERT statement ---");
// const insertSql = 'insert into users (user_id, username, dept_id, building, phone_number) values (10, "fred", "ACCT", "central", "800-867-5309")';
// console.log(JSON.stringify(parseSql(insertSql), null, 2));

// console.log("--- Parsing CREATE statements ---");
// const createSql = `
//   CREATE TABLE users (
//       user_id INTEGER,
//       username TEXT,
//       dept_id INTEGER,
//       building TEXT,
//       phone_number TEXT,
//       CONSTRAINT fk__view_as__department_name 
//       FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
//       INDEX idx_username (username),
//       INDEX idx_dept_id (dept_id)
//   )
// `;
// console.log(JSON.stringify(parseSql(createSql), null, 2));

// const createSql2 = `
//   CREATE TABLE departments (
//       dept_id TEXT,
// 	  department_name TEXT,
//       city TEXT,
//       INDEX idx_dept_id (dept_id)
//   )
// `;
// console.log(JSON.stringify(parseSql(createSql2), null, 2));

// console.log("\n--- Parsing SELECT statement ---");
// const selectSql = 'select username, phone_number from users where username = "fred" and (department = "accounting" or building = "central")';
// console.log(JSON.stringify(parseSql(selectSql), null, 2));

