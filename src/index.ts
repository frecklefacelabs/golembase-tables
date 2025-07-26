import { Annotations, transformListPOJOToAnnotations, transformPOJOToAnnotations } from "@freckleface/golembase-js-transformations";
import { AccountData, Annotation, createClient, GolemBaseCreate, Tagged } from "golem-base-sdk";
import { parseSql, SQLCreateTableToGBCreate } from "./sql_to_object.js";
import { readFileSync } from "fs";

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
	const create = SQLCreateTableToGBCreate('GOLEM-SQLTEST-v0.1',
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
	);
	console.log(create);

	const create2 = SQLCreateTableToGBCreate('GOLEM-SQLTEST-v0.1',
		`CREATE TABLE departments (
		    dept_id INTEGER,
			department_name TEXT,
			city TEXT,
			INDEX idx_dept_id (dept_id),
			INDEX idx_department_name (department_name)
		)
		`
	)
	console.log(create2);

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
// 	let tableNames: string = indexes.map((item) => {
// 		return item.tablename;
// 	}).join(',');

// 	// the tablenames member will include the tablenames as a single comma-separated string.
// 	// If a tablename contains anything but letters, numbers, underscores, dashes, throw an error.
// 	bigObj.tablenames = tableNames;  // Need to create an interface; this gives me error: Property 'tablenames' does not exist on type '{}' (unless I throw "any" after let bigObj)

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

