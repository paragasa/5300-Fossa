#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include "db_cxx.h"

// include the sql parser
//TODO: change this before submitting with an update to make file
#include "/usr/local/db6/include/SQLParser.h"

// contains printing utilities
// TODO: change this before submitting with an update to make file
#include "/usr/local/db6/include/sqlhelper.h"

using namespace std;

string execute(hsql::SQLParserResult* result);
string handleOperatorExpression(hsql::Expr* expr);
string handleExpression(hsql::Expr* expr);
string handlePrintSelect(const hsql::SelectStatement* statement);
string handlePrintCreate(const hsql::CreateStatement* statement);


int main(int argc, char* argv[])
{
  string cmd, path, statement;

  // Ensure user provides path
  if(argc < 2)
  {
    fprintf(stderr, "Usage: ./cpsc5300 [path to a writable directory]\n");
    return -1;
  }

  // Set path to the first argument provided by the user
  path = argv[1];

  //Initialize DBenv flags
  u_int32_t env_flags = DB_CREATE |     // If the environment does not
                                        // exist, create it.
                      DB_INIT_MPOOL; // Initialize the in-memory cache.

  string envHome(path);
  DbEnv myEnv(0U);

  try {
    myEnv.open(envHome.c_str(), env_flags, 0);
  } catch(DbException &e) {
      cerr << "Error opening database environment: "
                << envHome << std::endl;
                cerr << e.what() << std::endl;
      exit( -1 );
  } catch(exception &e) {
    std::cerr << "Error opening database environment: "
              << envHome << std::endl;
    std::cerr << e.what() << std::endl;
    exit(-1);
  }


  // Begin control loop
  printf("'quit' to exit\n");
  while(true)
  {
    printf("SQL> ");
    getline(cin, cmd);

    if(cmd == "quit")
    {
      return 0;
    }

    // parse a given query
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(cmd);


    // check whether the parsing was successful
    if (result->isValid()) {
      statement = execute(result);

      //cout << "state";
      cout << statement << endl;
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
      return -1;
    }
  }
}

string execute(hsql::SQLParserResult* result)
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
      default:
        cout << "This SQLStatement is not yet handled \n";
        break;
    }

  }

  return finalQuery;
}

 string handleOperatorExpression(hsql::Expr* expr) {
  string rtrnQuery = "";

  if (expr == NULL) {
    return "null";
  }

  switch (expr->opType) {
    case hsql::Expr::SIMPLE_OP:
      rtrnQuery += expr->opChar;
    case hsql::Expr::AND:
      rtrnQuery += "AND";
      break;
    case hsql::Expr::OR:
      rtrnQuery += "OR";
      break;
    case hsql::Expr::NOT:
      rtrnQuery += "NOT";
      break;
    default:
      rtrnQuery += expr->opType;
      break;
  }

  rtrnQuery += handleExpression(expr->expr);
  if (expr->expr2 != NULL) rtrnQuery += handleExpression(expr->expr2);

  return rtrnQuery;
}

string handleExpression(hsql::Expr* expr)
{
  string compoundStmt;

  switch (expr->type) {
    case hsql::kExprStar:
      return "*";
    case hsql::kExprColumnRef:
      return expr->name;
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
      compoundStmt += expr->expr->name;
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

string handlePrintSelect(const hsql::SelectStatement* statement)
{
  string query;

  query += "SELECT ";

  for (hsql::Expr* expr : *statement->selectList)
  {
    query += handleExpression(expr);
  }

  query += " FROM ";

  /*
  printTableRefInfo(stmt->fromTable, numIndent + 2);

    if (stmt->whereClause != NULL) {
      inprint("Search Conditions:", numIndent + 1);
      handleExpression(stmt->whereClause, numIndent + 2);
    }


    if (stmt->unionSelect != NULL) {
      inprint("Union:", numIndent + 1);
      printSelectStatementInfo(stmt->unionSelect, numIndent + 2);
    }

    if (stmt->order != NULL) {
      inprint("OrderBy:", numIndent + 1);
      handleExpression(stmt->order->at(0)->expr, numIndent + 2);
      if (stmt->order->at(0)->type == kOrderAsc) inprint("ascending", numIndent + 2);
      else inprint("descending", numIndent + 2);
    }

    if (stmt->limit != NULL) {
      inprint("Limit:", numIndent + 1);
      inprint(stmt->limit->limit, numIndent + 2);
    }
    */


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
      query += "TABLE ";
      break;
    //Add other creates here like create database
    default:
      fprintf(stderr, "Unsupported CREATE type %d\n", statement->type);
      break;
  }
  if(statement->ifNotExists)
  {
    query += "IF NOT EXISTS ";
  }

  //Create table specific stuff
  if(statement->tableName != NULL)
  {
    query += statement->tableName;
    query += " (";
  }


  if(statement->columns !=NULL)
  {
    bool firstColumn = true;
    for (hsql::ColumnDefinition* column : *statement->columns) {
        if(firstColumn)
        {
          firstColumn=false;
        }
        else{
          query +=", ";
        }
        query += column->name;
        switch(column->type)
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

  return query;
}
