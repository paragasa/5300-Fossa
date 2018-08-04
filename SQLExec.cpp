/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Kevin Lundeen, Minh Nguyen, Amanda Iverson
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include <algorithm>
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
	if (qres.column_names != nullptr) {
		for (auto const &column_name : *qres.column_names)
			out << column_name << " ";
		out << endl << "+";
		for (unsigned int i = 0; i < qres.column_names->size(); i++)
			out << "----------+";
		out << endl;
		for (auto const &row : *qres.rows) {
			for (auto const &column_name : *qres.column_names) {
				Value value = row->at(column_name);
				switch (value.data_type) {
				case ColumnAttribute::INT:
					out << value.n;
					break;
				case ColumnAttribute::TEXT:
					out << "\"" << value.s << "\"";
					break;
				case ColumnAttribute::BOOLEAN:
					out << (value.n == 0 ? "false" : "true");
					break;

				default:
					out << "???";
				}
				out << " ";
			}
			out << endl;
		}
	}
	out << qres.message;
	return out;
}

QueryResult::~QueryResult() {
	if (column_names != nullptr)
		delete column_names;
	if (column_attributes != nullptr)
		delete column_attributes;
	if (rows != nullptr) {
		for (auto row : *rows)
			delete row;
		delete rows;
	}
}


QueryResult *SQLExec::execute(const SQLStatement *statement) throw(SQLExecError) {
	// initialize _tables and _indices table, if not yet present
	if (SQLExec::tables == nullptr) {
		SQLExec::tables = new Tables();
	}

	//if the indice doesn't exist...create a new one
	if (SQLExec::indices == nullptr) {
		SQLExec::indices = new Indices();
	}


	try {
		// determine the type of command that is being asked. call the appropriate
		// statement (crate, drop or show) dependent on the sql query
		switch (statement->type()) {
		case kStmtCreate:
			return create((const CreateStatement *)statement);
		case kStmtDrop:
			return drop((const DropStatement *)statement);
		case kStmtShow:
			return show((const ShowStatement *)statement);
		case kStmtInsert:
			return insert((const InsertStatement *) statement);
		case kStmtDelete:
			return del((const DeleteStatement *) statement);
		case kStmtSelect:
			return select((const SelectStatement *) statement);
		default:
			return new QueryResult("not implemented");
		}
	}
	catch (DbRelationError& e) {
		// tell the user they encountered a dbrelation error
		throw SQLExecError(string("DbRelationError: ") + e.what());
	}
}

QueryResult *SQLExec::insert(const InsertStatement *statement){
    Identifier tableName = statement->tableName;
    DbRelation& targetTable = SQLExec::tables->get_table(tableName);
    ColumnNames columns;
    ColumnAttributes attributes;
    string message;

    if(statement->columns != NULL){
	for(auto const& column : *statement->columns){
	    columns.push_back(column);
	}
    }
    else{
	columns = targetTable.get_column_names();
    }



    ValueDict row;
    int i = 0;
    for (auto const& value : *statement->values){
	if(value->type == kExprLiteralInt){
	    row[columns[i]] = Value(value->ival);
	}
	else{
	    row[columns[i]] = Value(value->name);
	}
	i++;
    }
    Handle handle = targetTable.insert(&row);

    IndexNames indices = SQLExec::indices->get_index_names(tableName);
    for (auto const& index : indices){
	DbIndex& newIndex = SQLExec::indices->get_index(tableName, index);
	newIndex.insert(handle);
    } //check this, how it is done in python but seems too short

    message += "Successfully inserted 1 row into " + tableName;
    if(!indices.empty()){
	message += " and from " + to_string(indices.size()) + " indices.";
    }
 
    return new QueryResult(message);
}

QueryResult *SQLExec::del(const DeleteStatement *statement){
    return new QueryResult("DELETE statement not yet implemented");
}

QueryResult *SQLExec::select(const SelectStatement *statement){
    return new QueryResult("SELECT statement not yet implemented");
}


void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
	ColumnAttribute& column_attribute) {
	//capture the column name
	column_name = col->name;

	// figure out the type of the column name and set the appropriate attribute
	switch (col->type)
	{
	case ColumnDefinition::TEXT:
		column_attribute.set_data_type(ColumnAttribute::TEXT);
		break;
	case ColumnDefinition::INT:
		column_attribute.set_data_type(ColumnAttribute::INT);
		break;
		// nothing for double for now....
	case ColumnDefinition::DOUBLE:

	default:
		throw SQLExecError("Unsupported data type");
	}
}

// Create command. This will either create a table or an indice. 
QueryResult *SQLExec::create(const CreateStatement *statement) {
	switch (statement->type) {
	case CreateStatement::kTable:
		return create_table(statement);
	case CreateStatement::kIndex:
		return create_index(statement);
		// if the user tries to "create" anything other than a table or indice, remind them
		// that we dont have that functionality. 
	default:
		return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
	}
}

// The function to actually create the table. This function is also set to make sure
// that the statemetn is only for a table, and not anything else. 
QueryResult *SQLExec::create_table(const CreateStatement *statement) {
	//Double check if statement is CREATE
	if (statement->type != CreateStatement::kTable)
		return new QueryResult("Only handle CREATE TABLE");


	// get the name of the table to be created from the sql statement brought in. 
	Identifier tableName = statement->tableName;
	ValueDict row;
	// set the table name in the dictionary
	row["table_name"] = tableName;

	//update _tables schema
	Handle tHandle = SQLExec::tables->insert(&row);

	//Get columns to udate _columns schema
	ColumnNames colNames;
	Identifier colName;
	ColumnAttributes colAttrs;
	ColumnAttribute colAttr;

	//iterate through the columns that the statement has specified and set
	// the definitions and save them into the colNames vector and colAttr vectors
	for (ColumnDefinition* column : *statement->columns) {
		column_definition(column, colName, colAttr);
		colNames.push_back(colName);
		colAttrs.push_back(colAttr);
	}

	try {
		//update _columns schema
		Handles cHandles;
		DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);

		try {
			for (unsigned int i = 0; i < colNames.size(); i++) {
				row["column_name"] = colNames[i];
				row["data_type"] = Value(colAttrs[i].
					get_data_type() == ColumnAttribute::INT ? "INT" : "TEXT");
				cHandles.push_back(cols.insert(&row));
			}
			//Actually create the table (relation)
			DbRelation& table = SQLExec::tables->get_table(tableName);
			//Check which CREATE type
			if (statement->ifNotExists)
				table.create_if_not_exists();
			else
				table.create();
		}
		catch (exception& e) {
			// attempt to remove from _columns
			try {
				for (unsigned int i = 0; i < cHandles.size(); i++) {
					cols.del(cHandles.at(i));
				}
			}
			catch (...) {}
			throw;
		}
	}
	catch (exception& e) {
		try {
			//attempt to remove from _tables
			SQLExec::tables->del(tHandle);
		}
		catch (...) {}
		throw;
	}

	return new QueryResult("created " + tableName);
}

//M4 Create index method to create new table given query user provide
QueryResult *SQLExec::create_index(const CreateStatement *statement) {

	//Double check if statement is CREATE INDEX
	if (statement->type != CreateStatement::kIndex)
		return new QueryResult("Only handle CREATE INDEX");

	Identifier table_name = statement->tableName;
	ColumnNames column_names;
	Identifier index_name = statement->indexName;  //variable type might change
	Identifier index_type;
	bool is_unique;

	//Add to schema: _indices
	ValueDict row;

	row["table_name"] = table_name;

	try {
		index_type = statement->indexType;
	}
	catch (exception& e) {
		index_type = "BTREE";
	}


	if (index_type == "BTREE") {
		is_unique = true;
	}
	else {
		is_unique = false;
	}

	// setup and save the specific information about the row
	row["table_name"] = table_name;
	row["index_name"] = index_name;
	row["seq_in_index"] = 0;
	row["index_type"] = index_type;
	row["is_unique"] = is_unique;

	Handles iHandles;
	//Catching error when inserting each row to _indices schema table
	try {
		for (auto const& col : *statement->indexColumns) {
			row["seq_in_index"].n += 1;
			row["column_name"] = string(col);
			iHandles.push_back(SQLExec::indices->insert(&row));
		}


		DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
		index.create();
	}
	catch (exception& e) {
		try {
			for (unsigned int i = 0; i < iHandles.size(); i++) {
				SQLExec::indices->del(iHandles.at(i));
			}
		}
		catch (...) {

		}

		throw;
	}

	return new QueryResult("created index " + index_name);

}



// DROP ... 
// determine if we are dropping a table or an index
QueryResult *SQLExec::drop(const DropStatement *statement) {
	switch (statement->type) {
	case DropStatement::kTable:
		return drop_table(statement);
	case DropStatement::kIndex:
		return drop_index(statement);
	default:
		return new QueryResult("Only DROP TABLE and CREATE INDEX are implemented");
	}
}

// DROP table function to remove the table from the database
QueryResult *SQLExec::drop_table(const DropStatement *statement) {
	//Double check if statement is DROP
	if (statement->type != DropStatement::kTable)
		return new QueryResult("Only handle DROP TABLE");

	Identifier tbName = statement->name;
	//Check if the request is to drop _tables or _columns table.
	//CAN'T remove these tables since they are schema tables
	if (tbName == Tables::TABLE_NAME || tbName == Columns::TABLE_NAME)
		throw SQLExecError("Can't drop a schema table");

	//get the table to drop
	DbRelation& tb = SQLExec::tables->get_table(tbName);

	ValueDict where;
	// get the table name, columns, and handles for the table
	where["table_name"] = Value(tbName);
	DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
	Handles* handles = cols.select(&where);


	//M4 remove indices
	Handles* index_handles = SQLExec::indices->select(&where);
	vector<Value> dropIndices;

	// iterate through all handles and add each index to the vector for removal
	for (unsigned int i = 0; i < index_handles->size(); i++) {
		ValueDict* index_attributes = SQLExec::indices->project(index_handles->at(i));
		// add each of the indices to the vector for removal
		dropIndices.push_back(index_attributes->at("index_name"));
	}

	// start the removal process....

	// This is removing the index from the berkley db
	for (unsigned int i = 0; i < dropIndices.size(); i++) {
		DbIndex& index = SQLExec::indices->get_index(tbName, dropIndices.at(i).s);
		// get each 
		index.drop();
	}

	// remove all the index's from the schema_table
	for (unsigned int i = 0; i < index_handles->size(); i++) {
		SQLExec::indices->del(index_handles->at(i));
	}

	// remove metadata about columns of this table in _columns schema table
	for (unsigned int i = 0; i < handles->size(); i++) {
		cols.del(handles->at(i));
	}

	//Handles memory because select method returns the "new" pointer
	// to prevent potential memory leak without. 
	//declared in heap
	delete handles;
	delete index_handles;

	//remove table 
	tb.drop();

	//remove metadata about this table in _tables schema table
	SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

	return new QueryResult("dropped " + tbName); // FIXME
}

//M4 Drop Index
QueryResult *SQLExec::drop_index(const DropStatement *statement) {

	//Double check if statement is DROP Index
	if (statement->type != DropStatement::kIndex)
		return new QueryResult("Only handle DROP INDEX");


	// get the table name and index name from the sql statement being brought in
	Identifier table_name = statement->name;
	Identifier index_name = statement->indexName;
	// get the index from the database 
	DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
	ValueDict where;
	// set the values 
	where["table_name"] = table_name;
	where["index_name"] = index_name;


	Handles* index_handles = SQLExec::indices->select(&where);
	index.drop();
	// iterate through the index handles and remove each indice
	for (unsigned int i = 0; i < index_handles->size(); i++) {
		SQLExec::indices->del(index_handles->at(i));
	}


	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete index_handles;

	return new QueryResult("dropped index " + index_name);
}

// Show function redirects to the appropraite method dependent on what the user chose
QueryResult *SQLExec::show(const ShowStatement *statement) {
	//sort out whether the statement is "SHOW TABLE.." or "SHOW COLUMNS .."
	switch (statement->type) {
	case ShowStatement::kTables:
		return show_tables();
	case ShowStatement::kColumns:
		return show_columns(statement);
	case ShowStatement::kIndex:
		return show_index(statement);
	default:
		throw SQLExecError("unsupported SHOW type");
	}

}

// Function to show all of the tables in the database

QueryResult *SQLExec::show_tables() {
	ColumnNames* colNames = new ColumnNames;
	colNames->push_back("table_name");


	ColumnAttributes* colAttrs = new ColumnAttributes;
	colAttrs->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	Handles* handles = SQLExec::tables->select();

	// -3 to discount schema table, schema columns, and schema indices
	u_long rowNum = handles->size() - 3;

	ValueDicts *rows = new ValueDicts;

	//Use project method to get all entries of table names
	for (unsigned int i = 0; i < handles->size(); i++) {
		ValueDict* row = SQLExec::tables->project(handles->at(i), colNames);
		Identifier tbName = row->at("table_name").s;

		//if table is not the schema table or column schema table, include in results
		if (tbName != Tables::TABLE_NAME && tbName != Columns::TABLE_NAME && tbName != Indices::TABLE_NAME)
			rows->push_back(row);
	}

	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete handles;

	return new QueryResult(colNames, colAttrs, rows,
		"successfully returned " + to_string(rowNum) + " rows"); // FIXME
}

// Function gets called when query requests to show the columns of a specific table
// Example query: show columns from goober
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
	DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);

	//Prepare column headers
	ColumnNames* colNames = new ColumnNames;
	colNames->push_back("table_name");
	colNames->push_back("column_name");
	colNames->push_back("data_type");

	ColumnAttributes* colAttrs = new ColumnAttributes;
	colAttrs->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	ValueDict where;
	where["table_name"] = Value(statement->tableName);
	Handles* handles = cols.select(&where);
	u_long rowNum = handles->size();

	ValueDicts* rows = new ValueDicts;

	//Use project method to get all entries of column names of the table
	// iterate through the handles of the specific table targeted
	for (unsigned int i = 0; i < handles->size(); i++) {
		// get each column name and teh data type from the table.
		// an example would be to return "x (int)" "y(int)" "z(int)" on goober.  
		ValueDict* row = cols.project(handles->at(i), colNames);
		// add each row to the rows vector
		rows->push_back(row);
	}

	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete handles;

	//return the QR of all the information about the columns. This 
	// will get ouptutted to the terminal for the user to see the progress
	return new QueryResult(colNames, colAttrs, rows,
		" successfully returned " + to_string(rowNum) + " rows");
}

//M4 Show Index 

// example Command: CREATE index fx on goober(x,y)
QueryResult *SQLExec::show_index(const ShowStatement *statement) {

	Identifier table_name = statement->tableName;

	//Prepare column header
	ColumnNames* column_names = new ColumnNames;

	// table name: i.e. goober
	column_names->push_back("table_name");

	// index name: i.e. fx
	column_names->push_back("index_name");

	// column name: i.e. x
	column_names->push_back("column_name");

	// sequence : i.e. 1 (this was the first colmn specified in the paranthesis above)
	column_names->push_back("seq_in_index");

	// this will be a BTREE or a Hash. In example above this would be a BTREE (we are not implementing a hash)
	column_names->push_back("index_type");

	// make sure to flag that this index shouold be unique. in the above....we set it to true by default
	column_names->push_back("is_unique");


	ValueDict where;
	//set the table name in the VD of where
	where["table_name"] = Value(statement->tableName);

	// Get the blockids and record ides of the indices for that specific table specified. 
	Handles* handles = SQLExec::indices->select(&where);

	// the row numbers need to be equal to the number of handles we have
	u_long rowNum = handles->size();

	ValueDicts* rows = new ValueDicts;
	//Use project method to get all entries of column names of the table
	for (unsigned int i = 0; i < handles->size(); i++) {
		ValueDict* row = SQLExec::indices->project(handles->at(i), column_names);
		rows->push_back(row);
	}

	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete handles;

	//the Query result will return the name of the index's information 
	// along wtih the 2 rows we constructed (x and y)
	return new QueryResult(column_names, nullptr, rows,
		" successfully returned " + to_string(rowNum) + " rows");

}

