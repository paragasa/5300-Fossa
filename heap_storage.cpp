/* heap_storage.cpp
* Jared Mead, John Nguyen
*/

#include "db_cxx.h"
#include "heap_storage.h"
using namespace std;
#include "storage_engine.h"
#include <stdio.h>
#include <cstring>


typedef u_int16_t RecordID;
typedef vector<RecordID> RecordIDs;
static const uint BLOCK_SZ = 4096;


//constructor
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
  //if this is a new block
  if(is_new)
  {
    //Intialize protected variables
    this->num_records = 0;
    this->end_free  = DbBlock::BLOCK_SZ -1;
    //default header creation
    put_header();
  }
  //if the block was already initialized return state information via header
  else
  {
    get_header(this->num_records,this->end_free);
  }

}



//puts an integer n at the specified offset
void SlottedPage::put_n(u_int16_t offset, u_int16_t n)
{
  *(u_int16_t*)this->address(offset) = n;
}

//gets an integer n at the specified offset
u_int16_t SlottedPage::get_n(u_int16_t offset)
{
  return *(u_int16_t*)this->address(offset);
}

//two returnparameters size and loc
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
  size = get_n(4*id);
  loc = get_n(4*id+2);
}
//Stores the id and size of header to a location
//Defaults to store the header of the entire block when using put_header
void SlottedPage::put_header(RecordID id, u_int16_t size, u_int16_t loc){
  //when default
  if(id == 0)
  {
    size = this->num_records;
    loc = this->end_free;
  }
  //sets the size to the address of the header for the record
  put_n(4*id, size);
  //sets the two bytes after the size
  put_n(4*id +2, loc);

}

//function to check if the block can fit the size
bool SlottedPage::has_room(u_int16_t size)
{
  //calculates number of records plus header
  u_int16_t available = (end_free - (num_records + 1)) * 4;
  //if size of new data is smaller or equal to available, we can add it
  return (size <= available);
}


//returns a void pointer with address of the data + offset
void* SlottedPage::address(u_int16_t offset)
{
  return (void*)((char*)this->block.get_data() + offset);
}

//returns only the non deleted pins
RecordIDs* SlottedPage::ids()
{
  //create a new vector of recordIDs
  RecordIDs* results = new RecordIDs;
  //loop through all created records
  for(RecordID id=1; id<=num_records; id++)
  {
    //get size and location of each record
    u_int16_t size;
    u_int16_t loc;
    get_header(size,loc,id);
    //check if deleted, size will be 0 if deleted
    if(size != 0)
    {
      //append the id to the end.
      results->push_back(id);
    }
  }
  //return the pointer to the vector
  return results;
}

//Two cases:
//start< end removes data from start up to end
//end<start makes room for extra data from end to start
void SlottedPage::slide(u_int16_t start, u_int16_t end)
{
  u_int16_t shift = end - start;
  if(shift == 0)
    return;
  //slide dataype

  RecordIDs* records = this->ids();

  for(RecordID record_id : *records)
  {
    u_int16_t size;
    u_int16_t loc;
    get_header(size,loc,record_id);

  }
}

//adds data as a new record to the slotted page block
RecordID SlottedPage::add(const Dbt* data) throw(DbBlockNoRoomError)
{
  //check to see if th block is full
  if(!has_room(data->get_size()))
  {
    throw DbBlockNoRoomError("not enough room to add data");
  }

  //increment record count
  this->num_records++;
  //set return value to record count for ID
  RecordID id = this->num_records;

  u_int16_t size = data->get_size();

  //Decrement free space based on size of addition
  this->end_free -= size;
  u_int16_t loc = this->end_free +1;
  //updates the default header with new number and size
  put_header();

  //set the header at the end
  put_header(id,size,loc);

  // updates block with the data
  memcpy(this->address(loc),data->get_data(),size);

  return id;
}
// Takes in a record id, need to check the record size and return the data
Dbt* SlottedPage::get(RecordID record_id)
{

    //retrieve header
    u_int16_t size;
    u_int16_t loc;
    get_header(size,loc,record_id);
    //if the location is zero this means the record is deleted
    if(!loc == 0)
    {
      //construct berkeleyDB:Dbt with data
      Dbt* result = new Dbt(this->address(loc), size);
      return result;
    }
    else
    {
      return NULL; //need to do something if its deleted
    }
    return NULL;
}

//updates a record using an ID and data
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError)
{
  u_int16_t size;
  u_int16_t loc;
  get_header(size,loc,record_id);
  //can we only insert same sized data wtihout having to move things
  //Smaller data would fit I guess but that would leave empty space
  if(data.get_size() != size)
  {
    throw DbBlockNoRoomError("not enough room to add data");
  }

  //set the header at the end
  put_header(record_id,size,loc);
  // updates block with the data
  memcpy(this->address(loc),data.get_data(),size);

}

void SlottedPage::del(RecordID record_id)
{
  u_int16_t size;
  u_int16_t loc;

  get_header(size,loc,record_id);
  put_header(record_id,0 ,0);
  //compact the records space
  slide(loc, loc + size);
}

//END SLOTTED page

//START HeapFile
//constructor might be complete in the header
//HeapFile::HeapFile(string name) : DbFile(name), dbfilename(""), last(0), closed(true), db(_DB_ENV, 0)
//{}

//opens the berkeleyDB
//int Db::open(DbTxn *txnid, const char *file,
//    const char *database, DBTYPE type, u_int32_t flags, int mode);
//DB class API docs https://web.stanford.edu/class/cs276a/projects/docs/berkeleydb/api_cxx/db_open.html
void HeapFile::db_open(uint flags)
{
  //protected variable closed
  if (!this->closed)
  {
    //The db is already open
    return;
  }
  //create a db handle using db_create
  DB* db_ptr= this->db.get_DB();
  DB_ENV* dbenv_ptr = _DB_ENV->get_DB_ENV();
  db_create(&db_ptr, dbenv_ptr, flags);
  //Set the record length to the block size this is defined in storage_engine.h
  this->db.set_re_len(BLOCK_SZ);
  //set dbfilename
  //this->dbfilename  = path(_DB_ENV + this->name + ".db");
  //hardcoded for now
  this->dbfilename  = "../Data/poop.db";
  //python self.dbfilename = os.path.join(_DB_ENV, self.name + '.db')
  //Null txnid is not transaction protected, 0 mode is default
  this->db.open(nullptr ,this->dbfilename.c_str(),this->name.c_str(),DB_RECNO,(u_int32_t)flags,0);


  // This creates a statistical structure and stores it in stat
  // However stat does not appear to be a variable in the HeapFile class
  //(void*) stat;
  //this->db.stat(stat, DB_FAST_STAT);
  //this->last = stat['ndata'];

  this->closed = false;

}

//creates the physical Heapfile for DB
void HeapFile::create(void)
{
  //need to open the DB
  this->db_open(0U);
  //create a new block
  SlottedPage* block = this->get_new();
  this->put(block);
}

//delete heapfile function
void HeapFile::drop(void)
{

}

//Open File
void HeapFile::open(void)
{
  this->db_open(0U);
}
void HeapFile::close(void)
{

}
//given in the assignment description
//Allocates a block for the database files
// Returns a SlottedPage that is empty for managing records
SlottedPage* HeapFile::get_new(void)
{
  //char block[DB_BLOCK_SZ];
  char block[BLOCK_SZ];
  memset(block,0,sizeof(block));
  Dbt data(block,sizeof(block));
  //last is u_int32_t
  int block_id= ++this->last;
  Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
  SlottedPage* page = new SlottedPage(data, this->last, true);
  // write it out with initialization applied
  this->db.put(nullptr, &key, &data, 0);
  this->db.get(nullptr, &key, &data, 0);
  return page;
}
SlottedPage* HeapFile::get(BlockID block_id)
{
 return NULL;
}



//Handle for berkeleyDB Db::put
//int Db::put(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags);
void HeapFile::put(DbBlock* block)
{
  //get_block(),get_block_id are defined in storage_engine.h
  BlockID block_id = block->get_block_id();
  Dbt key(&block_id, sizeof(block_id));
  this->db.put(nullptr, &key,block->get_block(),0);
}

BlockIDs* HeapFile::block_ids()
{
  return NULL;
}

//END HEAPFILE
