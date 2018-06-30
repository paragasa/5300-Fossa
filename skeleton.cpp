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
    cin >> cmd;

    if(cmd == "quit")
    {
      return 0;
    }

    // parse a given query
    hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(cmd);


    // check whether the parsing was successful
    if (result->isValid()) {
      statement = execute(result);
      printf("%s\n", statement);
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
  return "It WORKS";
}
