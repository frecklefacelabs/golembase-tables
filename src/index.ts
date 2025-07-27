import { Annotations, transformAnnotationsToPOJO, transformListPOJOToAnnotations, transformPOJOToAnnotations } from "@freckleface/golembase-js-transformations";
import { AccountData, Annotation, createClient, GolemBaseCreate, Tagged } from "golem-base-sdk";
import { buildFkQueries, filterObjectBySelect, ParsedForeignKey, parseForeignKeyString, parseSql, SQLCreateTableToGBCreate, SQLInsertToGBCreate } from "./sql_to_object.js";
import { readFileSync } from "fs";

/**
 * TODO:
 * Convert all tablenames to lowercase, every time. The system is case-sensitive and if the users do create table departments and SELECT * FROM DEPARTMENTS, they won't match up.
*/

/**
 * Describes the structure of a single "view as" mapping object.
 */
interface ViewAsMapping {
  local_key: string;
  view_as: string;
}

/**
 * Defines the main data structure with required fields 
 * and support for dynamic, string-keyed properties.
 */
export interface Indexer {
  /** An array of table names. This property is always required. */
  tables: string[];

  /** An array of objects defining foreign key to view-as mappings. */
  view_as: ViewAsMapping[];

  /**
   * Index signature to allow for any other property name.
   * The value of these dynamic properties must be an array of strings.
   * The type `string[] | ViewAsMapping[]` ensures compatibility with the
   * explicitly defined 'tables' and 'view_as' properties.
   */
  [key: string]: string[] | ViewAsMapping[];
}

const encoder = new TextEncoder();
const decoder = new TextDecoder();


const keyBytes = readFileSync('./private.key');
const key: AccountData = new Tagged("privatekey", keyBytes);
export const client = await createClient(1337, key, 'http://localhost:8545', 'ws://localhost:8545');

export const test1 = async () => {

	// Todo: Check if table by this name already exists; if so, grab it and do an update/overwrite

	// todo: Need to handle situation of this being an update
	// const create = SQLCreateTableToGBCreate('GOLEM-SQLTEST-v0.1',
	// 	`CREATE TABLE users (
	// 		user_id INTEGER,
	// 		username TEXT,
	// 		dept_id INTEGER,
	// 		building TEXT,
	// 		phone_number TEXT,
	// 		CONSTRAINT fk__view_as__department_name 
	// 		FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
	// 		INDEX idx_username (username),
	// 		INDEX idx_dept_id (dept_id)
	// 	)`
	// );
	// console.log(create);

	// const create2 = SQLCreateTableToGBCreate('GOLEM-SQLTEST-v0.1',
	// 	`CREATE TABLE departments (
	// 	    dept_id INTEGER,
	// 		department_name TEXT,
	// 		city TEXT,
	// 		INDEX idx_dept_id (dept_id),
	// 		INDEX idx_department_name (department_name)
	// 	)`
	// )
	// console.log(create2);

	// const insert1 = SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO DEPARTMENTS (dept_id, department_name, city) values ('ACCT', 'Accounting', 'New York')");
	// console.log(insert1);

	// const insert2 = SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO DEPARTMENTS (dept_id, department_name, city) values ('IT', 'Information Technology', 'New York')");
	// console.log(insert2);

	// const insert3 = SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO DEPARTMENTS (dept_id, department_name, city) values ('MGT', 'Management', 'Boston')");
	// console.log(insert3);

	// const insert4 = SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO DEPARTMENTS (dept_id, department_name, city) values ('HR', 'Human Resources', 'Chicago')");
	// console.log(insert4);

	let creates: GolemBaseCreate[] = [

		SQLCreateTableToGBCreate('GOLEM-SQLTEST-v0.1',
			`CREATE TABLE users (
				user_id INTEGER,
				username TEXT,
				dept_id INTEGER,
				building TEXT,
				phone_number TEXT,
				CONSTRAINT fk__view_as__department_name 
				FOREIGN KEY (dept_id) REFERENCES departments(dept_id),
				INDEX idx_username (username),
				INDEX idx_dept_id (dept_id)
			)`
		),
		SQLCreateTableToGBCreate('GOLEM-SQLTEST-v0.1',
			`CREATE TABLE departments (
				dept_id INTEGER,
				department_name TEXT,
				city TEXT,
				INDEX idx_dept_id (dept_id),
				INDEX idx_department_name (department_name)
			)`
		),
		SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO departments (dept_id, department_name, city) values ('ACCT', 'Accounting', 'New York')"),
		SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO departments (dept_id, department_name, city) values ('IT', 'Information Technology', 'New York')"),
		SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO departments (dept_id, department_name, city) values ('MGT', 'Management', 'Boston')"),
		SQLInsertToGBCreate('GOLEM-SQLTEST-v0.1',"INSERT INTO departments (dept_id, department_name, city) values ('HR', 'Human Resources', 'Chicago')"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (101, 'asmith', 'ACCT', 'Main', '555-0101');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (102, 'bjones', 'MGT', 'West Wing', '555-0102');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (103, 'cwilliams', 'ACCT', 'Main', '555-0103');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (104, 'davis_r', 'HR', 'Annex', '555-0104');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (105, 'emiller', 'MGT', 'West Wing', '555-0105');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (106, 'fgarcia', 'IT', 'South Tower', '555-0106');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (107, 'h.chen', 'ACCT', 'Main', '555-0107');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (108, 'ijackson', 'HR', 'Annex', '555-0108');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (109, 'kim_s', 'IT', 'South Tower', '555-0109');"),
		SQLInsertToGBCreate("GOLEM-SQLTEST-v0.1", "INSERT INTO users (user_id, username, dept_id, building, phone_number) VALUES (110, 'l.taylor', 'MGT', 'West Wing', '555-0110');"),

	];

	// Joins happen automatically; they're built in via view-as constraints on foreign keys

	//const receipts = await client.createEntities(creates);
	//console.log(receipts);

	// Here's a good example of why we probably want to make calls into the library at some level
	// The call happens after prepping the SQL and before selecting out the fields
	//const selectSql2 = "select username, dept_id from users where building = 'Main'";
	const selectSql2 = "select username, dept_id from users where building = 'West Wing'";
	const selectSqlObj2 = parseSql(selectSql2);
	// Grab the tablename and grab its metadata
	// TODO: Store table's hash as well so we can grab it directly rather than query?
	const select_tables = await client.queryEntities(`app="GOLEM-SQLTEST-v0.1" && type="table" && tablename="${selectSqlObj2?.tablename}"`)
	let FKs: Record<string, ParsedForeignKey> = {};
	if (select_tables.length > 0) {
		// Grab the table's metadata
		const table_metadata = await client.getEntityMetaData(select_tables[0]?.entityKey);
		for (let pair of table_metadata.stringAnnotations) {
			const foundFk = parseForeignKeyString(pair.value);
			if (foundFk) {
				const keyname = foundFk.localKey as string;
				FKs[keyname] = foundFk;
			}
		}
	}

	const result2 = await client.queryEntities(selectSqlObj2?.where);
	for (let item of result2) {
		const metadata = await client.getEntityMetaData(item.entityKey);
		// Convert to a data object
		const obj = transformAnnotationsToPOJO(metadata);
		let final = filterObjectBySelect(selectSqlObj2?.select, obj);

		// Now for the foreign key view-as (if present)

		const builtFKs = buildFkQueries(final, FKs);
		if (builtFKs?.length > 0) {
			for (let fk of builtFKs) {
				// Query for the foreign key's item
				// 1. Query
				// 2. Grab metadata
				// 3. Convert to POJO

				const query_fk = await client.queryEntities(fk.queryString);

				// Grab the keyname to use (same as "view as") and store its value locally with the same name.
				// Should only return one, but just in case, just grab first

				if (query_fk && query_fk.length > 0) {
					const fk_metadata = await client.getEntityMetaData(query_fk[0].entityKey);
					const fk_pojo = transformAnnotationsToPOJO(fk_metadata);

					final[fk.viewKey] = fk_pojo[fk.viewKey];

				}
			}
		}

		console.log(final);
	}

	// Load the indexer entity (if present; if not, create the initial object)
	// Add on this table's indexes (username, dept_id) and update with current data
	// Note: Updating with current data requires reading all the table's "rows", but this is intended for small data sets

	// TODO: For this structure we save with comma-delimited strings but deconstruct to arrays.
	//       Maybe I should do the same for everything? Specifically the indexes portion of the create table
	// let indexer = {
	// 	tables: [
	// 		"users", "departments"
	// 	],
	// 	view_as: [
	// 		// This feels a little clumsy but I'll go with it for now. (I really don't want to update my transformer to support more complex structures.)
	// 		"tables_dept_id|departments_department_name"
	// 	],
	// 	"users_username": [
	// 		"fred", "susie", "malcolm"
	// 	],
	// 	"users_dept_id": [
	// 		// Let's enforce a rule that "view as" columns must also appear in this structure. Then the client can easily convert
	// 		"ACCT", "IT", "MGT", "HR"
	// 	],
	// 	"departments_dept_it": [
	// 		"ACCT", "IT", "MGT", "HR"
	// 	],
	// 	"departments_department_name": [
	// 		"Accounting", "IT", "Management", "Human Resources"
	// 	]
	// }

	/*let indexer = {
		tables: ["departments"],
		view_as: [ ],
		departments_dept_it: [ "ACCT", "IT", "MGT", "HR" ],
		departments_department_name: [ "Accounting", "IT", "Management", "Human Resources" ]
	};

	const indexerEntity: GolemBaseCreate = { data: encoder.encode('indexer'), btl: 100,...(transformListPOJOToAnnotations(indexer))};


	// Save the entities (If indexer already exists, use Update instead of Create)
	const receipts = await client.createEntities([create, indexerEntity]);
	console.log(receipts);
	*/
}

await test1();

// export interface TableIndex {
// 	tablename: string;
// 	indexes: string[];
// }

// export type TableIndexes = TableIndex[];

// export const prepareTableIndexes_take1 = (app: string, indexes: TableIndexes) => {
// 	let annots:Annotations = {
// 		stringAnnotations: [],
// 		numericAnnotations: []
// 	};
// 	annots.stringAnnotations.push({key: "app", value: app});
// 	annots.stringAnnotations.push({key: "type", value: "table_index"});

// 	// Prepare the big object before transforming it

// 	let bigObj: any = {	
// 	}

// 	// Grab the tablenames
// 	let tablenames: string = indexes.map((item) => {
// 		return item.tablename;
// 	}).join(',');

// 	// the tablenames member will include the tablenames as a single comma-separated string.
// 	// If a tablename contains anything but letters, numbers, underscores, dashes, throw an error.
// 	bigObj.tablenames = tablenames;  // Need to create an interface; this gives me error: Property 'tablenames' does not exist on type '{}' (unless I throw "any" after let bigObj)

// 	for (let index of indexes) {
// 		bigObj[index.tablename] = index.indexes.join(',');
// 	}

// 	console.log(bigObj);
// };

// prepareTableIndexes_take1(
// 	'my_test_app',
// 	[
// 		{
// 			tablename: "user",
// 			indexes: ["username", "department"]
// 		},
// 		{
// 			tablename: "department",
// 			indexes: ["dept_id"]
// 		}
// 	]
// )

// export const prepareTableIndexes = (app: string, indexes: any) => {

// 	// Make sure the member "tablenames" is not present; if so, throw an error
// 	if ('tablenames' in indexes || 'type' in indexes || 'app' in indexes) {
// 		throw new Error('Please do not include a member called tablenames, type, or app in your index object.')
// 	}

// 	// Copy the indexes object. Since this is just a simple object with strings, we can use the spread

// 	let indexCopy = { ...indexes };
	
// 	// Gather up member names; these are table names

// 	indexCopy.tablenames =  Object.keys(indexCopy).join(',');
// 	console.log(indexCopy);

// 	// Now convert to a Golem object and store it

// 	let annots:Annotations = transformPOJOToKeyValuePairs(indexCopy);
// 	annots.stringAnnotations.push(new Annotation("app", app));
// 	annots.stringAnnotations.push(new Annotation("type", "table_index"));

// 	console.log(annots);

// };

// export const addEntity_Test = () => {
// 	// After adding the entity:

// 	// Grab its table name (stored in "type")

// 	// Read in the indexes object -- read it as an entity, and then transform it (should that be combined into a function? Maybe add on to the transformers library to read and write?)

// 	// Query on all the objects in the table (this is where things get tricky, as it's not meant for huge datasets)
// 	// and conver them to plain old objects so we can use them

// 	// Loop through the index names and grab the values like so (assuming this is the user "table"):
// 	// [
// 	//    username: [
// 	//        "fred", "suzy", "jamal"
// 	//    ],
// 	//    department: [
// 	//    ]
// 	// ]
// 	//

// 	// ISSUE: Let's treat department as a foreign key and grab the actual department name from the department table?


// }

// prepareTableIndexes('my-test-app', {
//   //tablenames: 'asdf',
//   //app: 'Something or other',
//   user: 'username,department[fk:department:dept_id:dept_name]',
//   department: 'dept_id'
// });

