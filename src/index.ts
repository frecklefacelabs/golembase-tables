import { Annotations, transformPOJOToKeyValuePairs } from "@freckleface/golembase-js-transformations";
import { Annotation } from "golem-base-sdk";

export interface TableIndex {
	tablename: string;
	indexes: string[];
}

export type TableIndexes = TableIndex[];

export const prepareTableIndexes_take1 = (app: string, indexes: TableIndexes) => {
	let annots:Annotations = {
		stringAnnotations: [],
		numericAnnotations: []
	};
	annots.stringAnnotations.push({key: "app", value: app});
	annots.stringAnnotations.push({key: "type", value: "table_index"});

	// Prepare the big object before transforming it

	let bigObj: any = {	
	}

	// Grab the tablenames
	let tableNames: string = indexes.map((item) => {
		return item.tablename;
	}).join(',');

	// the tablenames member will include the tablenames as a single comma-separated string.
	// If a tablename contains anything but letters, numbers, underscores, dashes, throw an error.
	bigObj.tablenames = tableNames;  // Need to create an interface; this gives me error: Property 'tablenames' does not exist on type '{}' (unless I throw "any" after let bigObj)

	for (let index of indexes) {
		bigObj[index.tablename] = index.indexes.join(',');
	}

	console.log(bigObj);
};

prepareTableIndexes_take1(
	'my_test_app',
	[
		{
			tablename: "user",
			indexes: ["username", "department"]
		},
		{
			tablename: "department",
			indexes: ["dept_id"]
		}
	]
)

export const prepareTableIndexes = (app: string, indexes: any) => {

	// Make sure the member "tablenames" is not present; if so, throw an error
	if ('tablenames' in indexes || 'type' in indexes || 'app' in indexes) {
		throw new Error('Please do not include a member called tablenames, type, or app in your index object.')
	}

	// Copy the indexes object. Since this is just a simple object with strings, we can use the spread

	let indexCopy = { ...indexes };
	
	// Gather up member names; these are table names

	indexCopy.tablenames =  Object.keys(indexCopy).join(',');
	console.log(indexCopy);

	// Now convert to a Golem object and store it

	let annots:Annotations = transformPOJOToKeyValuePairs(indexCopy);
	annots.stringAnnotations.push(new Annotation("app", app));
	annots.stringAnnotations.push(new Annotation("type", "table_index"));

	console.log(annots);

};

export const addEntity_Test = () => {
	// After adding the entity:

	// Grab its table name (stored in "type")

	// Read in the indexes object -- read it as an entity, and then transform it (should that be combined into a function? Maybe add on to the transformers library to read and write?)

	// Query on all the objects in the table (this is where things get tricky, as it's not meant for huge datasets)
	// and conver them to plain old objects so we can use them

	// Loop through the index names and grab the values like so (assuming this is the user "table"):
	// [
	//    username: [
	//        "fred", "suzy", "jamal"
	//    ],
	//    department: [
	//    ]
	// ]
	//

	// ISSUE: Let's treat department as a foreign key and grab the actual department name from the department table?


}

prepareTableIndexes('my-test-app', {
  //tablenames: 'asdf',
  //app: 'Something or other',
  user: 'username,department[fk:department:dept_id:dept_name]',
  department: 'dept_id'
});

