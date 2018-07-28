/**
 * @file sql5300.cpp - implementation of main class 
 * @author Jared Mead, Johnny Nguyen, Minh Nguyen, Amanda Iverson
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include "db_cxx.h"
#include "heap_storage.h"
#include "storage_engine.h"
#include "SQLExec.h"
// include the sql parser
#include "ParseTreeToString.h"
#include "SQLParser.h"

// contains printing utilities

#include "sqlhelper.h"

using namespace std;

DbEnv *_DB_ENV;

void execute(hsql::SQLParserResult* result);
string handleOperatorExpression(hsql::Expr* expr);
string handleExpression(hsql::Expr* expr);
string handleTable(hsql::TableRef* table);
string handlePrintSelect(const hsql::SelectStatement* statement);
string handlePrintCreate(const hsql::CreateStatement* statement);
string handlePrintShow(const hsql::ShowStatement * statement);
string handlePrintDrop(const hsql::DropStatement * statement);
string handlePrintInsert(const hsql::InsertStatement * statement);

int main(int argc, char* argv[])
{
  string cmd, path, statement;

  //Ensure user provides path
  if(argc < 2)
  {
    fprintf(stderr, "Usage: ./sql5300 [path to a writable directory]\n");
    return -1;
  }

  // Set path to the first argument provided by the user
  path = argv[1];

  
  //Initialize DBenv flags
  u_int32_t env_flags = DB_CREATE |     // If the environment does not
                                        // exist, create it.
                      DB_INIT_MPOOL; // Initialize the in-memory cache.

  string envHome(path);
  DbEnv *myEnv = new DbEnv(0U);
  //MN: removed one exception block
  try {
    myEnv->open(envHome.c_str(), env_flags, 0);
  } catch(exception &e) {
    std::cerr << "Error opening database environment: "
              << envHome << std::endl;
    std::cerr << e.what() << std::endl;
    exit(-1);
  }

  _DB_ENV = myEnv;
  initialize_schema_tables();
   
  // Begin control loop
  printf("'quit' to exit\n");

  while(true){
          printf("SQL> ");
          getline(cin, cmd);

          if(cmd == "quit"){
              return 0;
		  }
		  else if (cmd == "test") {
			  cout << "Testing heap storage: " << test_heap_storage() << endl;
		  }
		  else {
			  // parse a given query
			  hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(cmd);


			  // check whether the parsing was successful
			  if (result->isValid()) {
				  execute(result);
				  delete result;
			  }
			  else
			  {
				  fprintf(stderr, "Given string is not a valid SQL query.\n");
				  fprintf(stderr, "%s (L%d:%d)\n",
					  result->errorMsg(),
					  result->errorLine(),
					  result->errorColumn());
				  delete result;
				  //MN: Continue the next loop, instead of disrupting the whole program
				  //return -1;
			  }
		  }

          

  }
  
  
  return 0;
}

// Main Driver, calls either select or create
void execute(hsql::SQLParserResult* result)
{
  string finalQuery;

  for (uint i = 0; i < result->size(); ++i) {
    // Print a statement summary.
    const hsql::SQLStatement* statement = result->getStatement(i);

    switch(statement->type())
    {
      case hsql::kStmtSelect:
        finalQuery = handlePrintSelect((const hsql::SelectStatement*)statement);
        break;
      case hsql::kStmtCreate:
        finalQuery = handlePrintCreate((const hsql::CreateStatement*) statement);
        break;
	  case hsql::kStmtDrop:
		  finalQuery = handlePrintDrop((const hsql::DropStatement*) statement);
		 break;
	  case hsql::kStmtShow:
		  finalQuery = handlePrintShow((const hsql::ShowStatement*) statement);
		  break;
	  case hsql::kStmtInsert:
		  finalQuery = handlePrintInsert((const hsql::InsertStatement*) statement);
		  break;
      default:
		finalQuery = "Unsupported query";
        break;
    }

	cout << finalQuery << endl;
	
	try {
		QueryResult *ret = SQLExec::execute(statement);
		cout << *ret << endl;
		delete ret;
	}
	catch (SQLExecError& e) {
		cout << "\nError: " << e.what() << endl;
	}

  }

 
}

// Handles operator expressions, accesses opType
 string handleOperatorExpression(hsql::Expr* expr) {
  string rtrnQuery = "";

  if (expr == NULL) {
    return "null";
  }

  rtrnQuery += handleExpression(expr->expr);

  switch (expr->opType) {
    case hsql::Expr::SIMPLE_OP:
      rtrnQuery += " ";
      rtrnQuery += expr->opChar;
      rtrnQuery += " ";
      break;
    case hsql::Expr::AND:
      rtrnQuery += " AND ";
      break;
    case hsql::Expr::OR:
      rtrnQuery += " OR ";
      break;
    case hsql::Expr::NOT:
      rtrnQuery += " NOT ";
      break;
    default:
      rtrnQuery += expr->opType;
      break;
  }

  if (expr->expr2 != NULL) rtrnQuery += handleExpression(expr->expr2);

  return rtrnQuery;
}

// Handles the Expr type
string handleExpression(hsql::Expr* expr)
{
  string compoundStmt;

  switch (expr->type) {
    case hsql::kExprStar:
      return "*";
    case hsql::kExprColumnRef:
      if(expr->table)
        return string(expr->table) + "." +  expr->name;
      else
        return string(expr->name);
    case hsql::kExprLiteralFloat:
      return to_string(expr->fval);
    case hsql::kExprLiteralInt:
      return to_string(expr->ival);
      break;
    case hsql::kExprLiteralString:
      return expr->name;
      break;
    case hsql::kExprFunctionRef:
      compoundStmt += expr->name;
      compoundStmt += " ";
      compoundStmt += handleExpression(expr->expr);
      return compoundStmt;
      break;
    case hsql::kExprOperator:
      return handleOperatorExpression(expr);
      break;
    default:
      fprintf(stderr, "Unrecognized expression type %d\n", expr->type);
      return " ";
      break;
  }
}

// Handles table commands, mainly Joins, but also handles
// Cross Product and Aliasing
string handleTable(hsql::TableRef* table)
{
  string compoundStmt;
  switch(table->type)
  {
      case hsql::kTableName:
        compoundStmt += table->name;
        if(table->alias)
          compoundStmt += string(" AS ") + table->alias;
        break;
      case hsql::kTableJoin:
          compoundStmt += handleTable(table->join->left);
          switch (table->join->type)
          {
            case hsql::kJoinInner:
              compoundStmt += " JOIN ";
              break;
            case hsql::kJoinLeft:
              compoundStmt += " LEFT JOIN ";
              break;
            case hsql::kJoinRight:
              compoundStmt += " RIGHT JOIN ";
              break;

                break;
           default:
              break;
          }
          compoundStmt += handleTable(table->join->right);
          if (table->join->condition)
            compoundStmt += " ON " + handleExpression(table->join->condition);
          break;
      case hsql::kTableCrossProduct:
        for (hsql::TableRef* tbl : *table->list)
            {
              compoundStmt += ", ";
              compoundStmt += handleTable(tbl);
            }
            break;
      default:
        fprintf(stderr, "Unrecognized expression type %d\n", table->type);
        return compoundStmt;
        break;
        break;
  }
  return compoundStmt;
}

//function that takes in a SQLStatement and returns the canonical format as a string
//for now this should only handle SELECT
string handlePrintSelect(const hsql::SelectStatement* statement)
{
  string query;

  query += "SELECT ";

  // Used to add commas
  bool firstColumn = true;

  for (hsql::Expr* expr : *statement->selectList)
  {
      if(firstColumn)
        {
          firstColumn=false;
        }
      else{
        query +=", ";
      }

      query += handleExpression(expr);
  }

   query += " FROM ";

   query += handleTable(statement->fromTable);

  if (statement->whereClause != NULL) {
    query += " WHERE ";
      query += handleExpression(statement->whereClause);
  }

  if (statement->order != NULL) {
    query += " ORDER BY ";
    query += handleExpression(statement->order->at(0)->expr);
                              if (statement->order->at(0)->type == hsql::kOrderAsc) query += " ASCENDING ";
                              else query += " DESCENDING ";
                              }

      if (statement->limit != NULL) {
        query += " LIMIT ";
        query += statement->limit->limit;
      }


  return query;
}



//function that takes in a SQLStatement and returns the canonical format as a string
//for now this should only handle CREATE TABLE
string handlePrintCreate(const hsql::CreateStatement* statement)
{
  string query = "CREATE ";
  switch(statement->type)
  {
    //Create for table
  case hsql::CreateStatement::kTable:
  {
	  query += "TABLE ";
	  if (statement->ifNotExists)
	  {
		  query += "IF NOT EXISTS ";
	  }

	  //Create table specific stuff
	  if (statement->tableName != NULL)
	  {
		  query += statement->tableName;
		  query += " (";
	  }


	  if (statement->columns != NULL)
	  {
		  bool firstColumn = true;
		  for (hsql::ColumnDefinition* column : *statement->columns) {
			  if (firstColumn)
			  {
				  firstColumn = false;
			  }
			  else {
				  query += ", ";
			  }
			  query += column->name;
			  switch (column->type)
			  {
			  case hsql::ColumnDefinition::TEXT:
				  query += " TEXT";
				  break;
			  case hsql::ColumnDefinition::INT:
				  query += " INT";
				  break;
			  case hsql::ColumnDefinition::DOUBLE:
				  query += " DOUBLE";
				  break;

			  default:
				  fprintf(stderr, "Unsupported Column type %d\n", column->type);
				  break;
			  }

		  }
		  query += ")";
	  }
  }
	  break;
    
	//Create for index
  case hsql::CreateStatement::kIndex:
	{
		query += "INDEX ";
		query += string(statement->indexName) + " ON ";
		query += string(statement->tableName) + " USING " + statement->indexType
			+ " (";
		bool doComma = false;
		for (auto const& col : *statement->indexColumns) {
			if (doComma)
				query += ", ";
			query += string(col);
			doComma = true;
		}
		query += ")";
	}
		break;

    default:
      fprintf(stderr, "Unsupported CREATE type %d\n", statement->type);
      break;
  
 
  }

  return query;
}

//function that takes in a SQLStatement and returns the canonical format as a string
//for now this should handle SHOW statements
string handlePrintShow(const hsql::ShowStatement* statement)
{
	string query = "SHOW ";
	switch (statement->type) {
	case hsql::ShowStatement::kTables:
		query += "TABLES";
		break;
	case hsql::ShowStatement::kColumns:
		query += "COLUMNS FROM " + string(statement->tableName);
		break;
	case hsql::ShowStatement::kIndex:
		query += "INDEX FROM " + string(statement->tableName);
		break;
	default:
		query += "??";
		break;
	}
	return query;
}

//function that takes in a SQLStatement and returns the canonical format as a string
//for now this should handle DROP statements
string handlePrintDrop(const hsql::DropStatement* statement)
{
	string query = "DROP ";
	switch (statement->type) {
	case hsql::DropStatement::kTable:
		query += "TABLE ";
		break;
	case hsql::DropStatement::kIndex:
		query += "INDEX " + string(statement->indexName) + " FROM ";
		break;
	default:
		query += "? ";
	}
	query += statement->name;
	return query;
}

//FIX ME: function that takes in a SQLStatement and returns the canonical format as a string
//for now this should handle INSERT statements
string handlePrintInsert(const hsql::InsertStatement* statement)
{
	string query = "INSERT ...";
	return query;
}