/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen, Minh Nguyen, Amanda Iverson
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;
Indices* SQLExec::indices = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
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

	if (SQLExec::indices == nullptr) {
		SQLExec::indices = new Indices();
	}
		

    try {
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

void SQLExec::column_definition(const ColumnDefinition *col, Identifier& column_name,
                                ColumnAttribute& column_attribute) {
	column_name = col->name;
	switch (col->type)
	{
	case ColumnDefinition::TEXT:
		column_attribute.set_data_type(ColumnAttribute::TEXT);
		break;
	case ColumnDefinition::INT:
		column_attribute.set_data_type(ColumnAttribute::INT);
		break;
	case ColumnDefinition::DOUBLE:

	default:
		throw SQLExecError("Unsupported data type");
	}
}

QueryResult *SQLExec::create(const CreateStatement *statement) {
	switch (statement->type) {
	case CreateStatement::kTable:
		return create_table(statement);
	case CreateStatement::kIndex:
		return create_index(statement);
	default:
		return new QueryResult("Only CREATE TABLE and CREATE INDEX are implemented");
	}
}

QueryResult *SQLExec::create_table(const CreateStatement *statement) {
	//Double check if statement is CREATE
	if (statement->type != CreateStatement::kTable)
		return new QueryResult("Only handle CREATE TABLE");

	//update _tables schema
	Identifier tableName = statement->tableName;
	ValueDict row;
	row["table_name"] = tableName;
	Handle tHandle = SQLExec::tables->insert(&row);

	//Get columns to udate _columns schema
	ColumnNames colNames;
	Identifier colName;
	ColumnAttributes colAttrs;
	ColumnAttribute colAttr;
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
			//Actual create the table (relation)
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

//M4 (MN): Create index method to create new table given query user provided
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
	SQLExec::indices->insert(&row);
	cout << "Before loop" << endl;
	for (ColumnDefinition *col : *statement->columns) {
		column_names.push_back(col->name);
	}

	try {
		index_type = statement->indexType;
	}
	catch (exception& e) {
		index_type = "BTREE";
	}

	/**
	try {
		is_unique = bool(statement->unique);
	}
	catch (exception& e) {
		is_unique = false;
	}*/

	if (index_type == "BTREE") {
		is_unique = true;
	}
	else {
		is_unique = false;
	}

	
	row["table_name"] = table_name;
	row["index_name"] = index_name;
	row["seq_in_index"] = 0;
	row["index_type"] = index_type;
	row["is_unique"] = is_unique;

	for (unsigned int i = 0; i < column_names.size(); i++) {
		row["seq_in_index"].n += 1;
		row["column_name"] = column_names.at(i);
	}

	DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
	//To check if index already exists?
	index.create();
	return new QueryResult("created " + index_name);

}



// DROP ...
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

// DROP ...
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
	where["table_name"] = Value(tbName);
	DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
	Handles* handles = cols.select(&where);


	//M4 (MN): remove indices
	//DbRelation& indices = SQLExec::tables->get_table(Indices::TABLE_NAME);
	Handles* index_handles = SQLExec::indices->select(&where);
	vector<Value> dropIndices;
	for (unsigned int i = 0; i < index_handles->size(); i++) {
		ValueDict* index_attributes = SQLExec::indices->project(index_handles->at(i));
		dropIndices.push_back(index_attributes->at("index_name"));
	}
	for (unsigned int i = 0; i < dropIndices.size(); i++) {
		DbIndex& index = SQLExec::indices->get_index(tbName, dropIndices.at(i).s);
		index.drop();
	}
	for (unsigned int i = 0; i < index_handles->size(); i++) {
		SQLExec::indices->del(index_handles->at(i));
	}

	// remove metadata about columns of this table in _columns schema table
	for (unsigned int i = 0; i < handles->size(); i++) {
		cols.del(handles->at(i));
	}
	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete handles;
	delete index_handles;

	//remove table
	tb.drop();

	//remove metadata about this table in _tables schema table
	SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

	return new QueryResult("dropped " + tbName); // FIXME
}

//M4 (MN): Drop Index
QueryResult *SQLExec::drop_index(const DropStatement *statement) {
	//Double check if statement is DROP Index
	if (statement->type != DropStatement::kIndex)
		return new QueryResult("Only handle DROP INDEX");


	Identifier table_name = statement->name;
	Identifier index_name = statement->indexName;
	
	DbIndex& index = SQLExec::indices->get_index(table_name, index_name);
	ValueDict where;
	where["table_name"] = table_name;
	where["index_name"] = index_name;
	Handles* index_handles = SQLExec::indices->select(&where);
	
	for (unsigned int i = 0; i < index_handles->size(); i++) {
		SQLExec::indices->del(index_handles->at(i));
	}
	index.drop();

	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete index_handles;

	return new QueryResult("dropped index " + index_name);
}

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

QueryResult *SQLExec::show_tables() {
	ColumnNames* colNames = new ColumnNames;
	colNames->push_back("table_name");

	ColumnAttributes* colAttrs = new ColumnAttributes;
	colAttrs->push_back(ColumnAttribute(ColumnAttribute::TEXT));

	Handles* handles = SQLExec::tables->select();
	u_long rowNum = handles->size() - 3; //-2 to discount schema table, schema columns, and schema indices

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
	for (unsigned int i = 0; i < handles->size(); i++) {
		ValueDict* row = cols.project(handles->at(i), colNames);
		rows->push_back(row);
	}

	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete handles;
	
	return new QueryResult(colNames, colAttrs, rows,
		" successfully returned " + to_string(rowNum) + " rows");
}

//M4 (MN): Show Index
QueryResult *SQLExec::show_index(const ShowStatement *statement) {
	Identifier table_name = statement->tableName;

	//Prepare column header
	ColumnNames* column_names = new ColumnNames;
	column_names->push_back("table_name");
	column_names->push_back("index_name");
	column_names->push_back("column_name");
	column_names->push_back("seq_in_index");
	column_names->push_back("index_type");
	column_names->push_back("is_unique");


	//DbRelation& indices = SQLExec::tables->get_table(Indices::TABLE_NAME);
	ValueDict where;
	where["table_name"] = Value(statement->tableName);
	Handles* handles = SQLExec::indices->select(&where);
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

	//Look at another form of QueryResult
	return new QueryResult(column_names, nullptr, rows,
		" successfully returned " + to_string(rowNum) + " rows");

}

