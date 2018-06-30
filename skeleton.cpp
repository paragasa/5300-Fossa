#include <stdlib.h>
#include <string>
#include <sys/types.h>
#include "db_cxx.h"

// include the sql parser
#include "SQLParser.h"

// contains printing utilities
#include "sqlhelper.h"

using namespace std;

string execute();

int main(int argc, char* argv[])
{
  string cmd, path;

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
  DbEnv myEnv(0);

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

    if(cin == "quit")
    {
      return 0;
    }

  }
}

string execute()
{

}
