/**
 * @file SQLExec.cpp - implementation of SQLExec class 
 * @author Kevin Lundeen
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

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
	// initialize _tables table, if not yet present
	if (SQLExec::tables == nullptr)
		SQLExec::tables = new Tables();

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
	


// DROP ...
QueryResult *SQLExec::drop(const DropStatement *statement) {
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

	//remove metadata about columns of this table in _columns schema table
	ValueDict where;
	where["table_name"] = Value(tbName);
	DbRelation& cols = SQLExec::tables->get_table(Columns::TABLE_NAME);
	Handles* handles = cols.select(&where);

	for (unsigned int i = 0; i < handles->size(); i++) {
		cols.del(handles->at(i));
	}
	//Handle memory because select method returns the "new" pointer
	//declared in heap
	delete handles;

	//remove table
	tb.drop();

	//remove metadata about this table in _tables schema table
	SQLExec::tables->del(*SQLExec::tables->select(&where)->begin());

	return new QueryResult("dropped " + tbName); // FIXME
}

QueryResult *SQLExec::show(const ShowStatement *statement) {
	//sort out whether the statement is "SHOW TABLE.." or "SHOW COLUMNS .."
	switch (statement->type) {
		case ShowStatement::kTables:
			return show_tables();
		case ShowStatement::kColumns:
			return show_columns(statement);
		case ShowStatement::kIndex:  //not yet implemented
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
	u_long rowNum = handles->size() - 2; //-2 to discount schema table and schema columns

	ValueDicts *rows = new ValueDicts;
	//Use project method to get all entries of table names
	for (unsigned int i = 0; i < handles->size(); i++) {
		ValueDict* row = SQLExec::tables->project(handles->at(i), colNames);
		Identifier tbName = row->at("table_name").s;
		//if table is not the schema table or column schema table, include in results
		if (tbName != Tables::TABLE_NAME && tbName != Columns::TABLE_NAME)
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

