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
//typedef vector<BlockID> BlockIDs;
static const uint BLOCK_SZ = 4096;

DbEnv* _DB_ENV;

/*
* constructor for the slotted page class
*/
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
  //if this is a new block we must initilize the variables
  if(is_new)
  {
    //Intialize protected variables
    this->num_records = 0;
    this->end_free  = DbBlock::BLOCK_SZ -1;
    //default header creation
    put_header();
  }
  //if the block was already initialized return state information via header of current block
  else
  {
    get_header(this->num_records,this->end_free);
  }

}

/*
*  puts an two byte integer n at the specified offset
*/
void SlottedPage::put_n(u_int16_t offset, u_int16_t n)
{
  *(u_int16_t*)this->address(offset) = n;
}

/*
* gets an integer at the specified offset
* This function was given in the documentation
*/
u_int16_t SlottedPage::get_n(u_int16_t offset)
{
  return *(u_int16_t*)this->address(offset);
}

/*
* Looks up a record by it's ID and returns header information size and location
* two out parameters size and loc
*/
void SlottedPage::get_header(u_int16_t &size, u_int16_t &loc, RecordID id)
{
  size = get_n(4*id);
  loc = get_n(4*id+2);
}

/*Stores the id and size of header to a location
* Defaults to store the header of the entire block when using put_header
*/
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
  //check to see if the end and start ar the same.
  if(shift == 0)
    return;

  //slide data

  //self.block[self.end_free + 1 + shift: end] = self.block[self.end_free + 1: start]
  //move the values from end_free +1 through start to end_free + 1 + shift to end.
  memcpy(this->address(end_free + 1), this->address(end_free + 1 + shift), shift);

  //for each record we must update headers with new locations after things have shifted
  RecordIDs* records = this->ids();
  for(RecordID record_id : *records)
  {
    u_int16_t size;
    u_int16_t loc;
    get_header(size,loc,record_id);
    //if the location is before the start we know we must shift and update headers
    if (loc <= start){
        loc+=shift;
        put_header(record_id,size,loc);
    }
    //self.end_free += shift
    this->end_free += shift;
    //update default get_header
    put_header();
  }
}

/*
*  This function adds data as a new record to the slotted page block
*/
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
/*
*  Takes in a record id, checks the record size and location
*  and returns the data
*/
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
    return nullptr; //need to do something if its deleted
  }
  return nullptr;
}

/*
* updates a record using an ID and data and puts it into the block.
*/
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError)
{
  u_int16_t size;
  u_int16_t loc;
  u_int16_t new_size = data.get_size();
  get_header(size,loc,record_id);
  //can we only insert same sized data wtihout having to move things
  //Smaller data would fit I guess but that would leave empty space
  //if(new_size != size)
  //{
  //  throw DbBlockNoRoomError("not enough room to add data");
  //}
  if(new_size > size)
  {
    u_int16_t extra = new_size - size;
    if(!has_room(extra))
      throw DbBlockNoRoomError("not enough room to add data");
    //slide first
    slide(loc + new_size, loc + size);
    memcpy(this->address(loc-extra),data.get_data(),new_size);
  }
  else
  {
    //copy first
    //self.block[loc:loc + new_size] = data
    memcpy(this->address(loc),data.get_data(),new_size);
    slide(loc + new_size, loc + size);
  }

  //set the header at the end
  put_header(record_id,size,loc);

}

// find the record ID within the block and sets the size and location to 0, then slides
void SlottedPage::del(RecordID record_id)
{
  u_int16_t size;
  u_int16_t loc;

  get_header(size,loc,record_id);
  put_header(record_id,0 ,0);
  //compact the records space
  slide(loc, loc + size);
}

//END SlottedPage

//START HeapFile
//constructor might is complete in the header
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
  //db_create(&db_ptr, _DB_ENV, flags);
  //Set the record length to the block size this is defined in storage_engine.h
  this->db.set_re_len(BLOCK_SZ);
  //set dbfilename

  this->dbfilename  = this->name + ".db";
  //python self.dbfilename = os.path.join(_DB_ENV, self.name + '.db')
  //Null txnid is not transaction protected, 0 mode is default
  this->db.open(nullptr ,this->dbfilename.c_str(),this->name.c_str(),DB_RECNO,(u_int32_t)flags,0);

  //MN: Added flag handler
  if (flags == 0) {
    	DB_BTREE_STAT stat;
		this->db.stat(nullptr, &stat, DB_FAST_STAT);
		this->last = stat.bt_ndata;
    } else {
    	this->last = 0;
  }

  this->closed = false;

}

//creates the physical Heapfile for DB
void HeapFile::create(void)
{
  //need to open the DB
  this->db_open(DB_CREATE);
  //create a new block
  SlottedPage* block = this->get_new();
  this->put(block);
}

//delete physical heapfile
void HeapFile::drop(void)
{
  this->close();
  //std function to remove a file based on path
  remove(dbfilename.c_str());
}

//Open File
void HeapFile::open(void)
{
  this->db_open();
  //block_size is set already
}

//closes the db file
void HeapFile::close(void)
{
  this->db.close(0);
  this->closed= true;
}

/*given in the assignment description
* Allocates a block for the database files
* Returns a SlottedPage that is empty for managing records
*/
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

//Retrieves a SlottedPage/Block, using a BlockID
SlottedPage* HeapFile::get(BlockID block_id)
{
  //allocate memory for the variable
  //Db::get(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags);
  Dbt key(&block_id, sizeof(block_id));
  Dbt data;
  this->db.get(nullptr, &key, &data, 0);
  SlottedPage* page = new  SlottedPage(data, block_id);
  return page;
}



//Handle for berkeleyDB Db::put
void HeapFile::put(DbBlock* block)
{
  //get_block(),get_block_id are defined in storage_engine.h
  BlockID block_id = block->get_block_id();
  Dbt key(&block_id, sizeof(block_id));
  //int Db::put(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags);
  this->db.put(nullptr, &key,block->get_block(),0);
}

//Returns a vector of BlockIds
BlockIDs* HeapFile::block_ids()
{
  BlockIDs* results = new BlockIDs;
  for(BlockID id=1; id<=this->last; id++)
  {
    results->push_back(id);
  }
  return results;
}

//END HeapFile

// BEGIN HeapTable
/*
 *  Corresponds to CREATE TABLE.
 *
 *  At minimum, it presumably sets up the DbFile and calls its create method.
 *
 *  Throws exception if the table already exists
 */
 HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes )
   : DbRelation(table_name, column_names, column_attributes), file(table_name)
 {
   // Used to handle Create vs Create if not exists
   isCreated = false;
 }

void HeapTable::create()
{
  if(!isCreated)
  {
    file.create();
    // Once the file is created if Create is called again an exception will
    // be thrown
    isCreated = true;
  }
  else
  {
    throw DbRelationError("TABLE IS ALREADY CREATED");
  }
}

/*
 *  Corresponds to CREATE TABLE IF NOT EXISTS.
 *
 *  Whereas create will throw an exception if the table already exists,
 *  this method will just open the table if it already exists.
 */
void HeapTable::create_if_not_exists()
{
  if(!isCreated)
  {
    file.create();
    isCreated = true;
  }
  else
  {
    file.open();
  }
}

/*
 *  Opens the table for insert, update, delete, select, and project methods
 */
void HeapTable::open()
{
  // Simply calls lower level function
  file.open();
}

/*
 *  Closes the table, temporarily disabling insert, update, delete, select,
 *  and project methods
 */
void HeapTable::close()
{
  // Simply calls lower level function
  file.close();
}

/*
 * Corresponds to DROP TABLE.
 *
 * Deletes the underlying DbFile.
 */
void HeapTable::drop()
{
  // Simply calls lower level function
  file.drop();
}

/*
 *  Corresponds to INSERT INTO TABLE.
 *
 *  Takes a proposed row and adds it to the table.
 *  This is the method that determines the block to write it to and marshals
 *  the data and writes it to the block.
 *  It is also responsible for handling any constraints, applying defaults, etc.
 *
 *  NOTE: FOR MILESTONE 2 INSERT ONLY HANDLES INTEGER AND TEXT, NULL VALUES
 *  OR ANY OTHER COLUMN ATTRIBUTES ARE NOT HANDLED
 */
Handle HeapTable::insert(const ValueDict* row)
{
  open();

  // Due to heap storage all that is needed to be done is validate and append
  // to the most recent block, may be different later
  return append(validate(row));
}


/*
 *  Corresponds to UPDATE.
 *
 *  Like insert, but only applies specific field changes,
 *  keeping other fields as they were before.
 *  Same logic as insert for constraints, defaults, etc.
 *  The client needs to first obtain a handle to the row that is meant to be
 *  updated either from insert or from select.
 *
 *  NOTE: FOR MILESTONE 2 UPDATE IS NOT SUPPORTED
 */
void HeapTable::update(const Handle handle, const ValueDict* new_values)
{
  printf("Update is not yet handled.");
}

/*
 *  Corresponds to DELETE FROM.
 *
 *  Deletes a row for a given row handle (obtained from insert or select).
 *
 *  NOTE: FOR MILESTONE 2 DELETE IS NOT SUPPORTED
 */
void HeapTable::del(const Handle handle)
{
  printf("Delete is not yet handled.");
}

/*
 *  Corresponds to SELECT * FROM
 *
 *  Returns handles to the matching rows.
 */
 Handles* HeapTable::select() {
     Handles* handles = new Handles();

     BlockIDs* block_ids = file.block_ids();

     for (auto const& block_id: *block_ids) {
         SlottedPage* block = file.get(block_id);
         RecordIDs* record_ids = block->ids();

         // returns ALL Rows
         for (auto const& record_id: *record_ids)
             handles->push_back(Handle(block_id, record_id));

         delete record_ids;
         delete block;
     }

     delete block_ids;

     return handles;
 }

/*
 *  Corresponds to SELECT * FROM ... WHERE
 *
 *  Returns handles to the matching rows.
 *
 *  NOTE: WHERE, GROUP BY, AND LIMIT ARE NOT YET HANDLED,
 *  THIS FUNCTION INSTEAD SIMPLY CALLS SELECT() WITHOUT ANY PARAMETERS
 */
Handles* HeapTable::select(const ValueDict* where)
{
  return select();
}

/*
 *   Extracts all fields from a row handle.
 */
ValueDict* HeapTable::project(Handle handle)
{
  // Get block id from handle
  BlockID block_id = get<0>(handle);
  // Get record id from handle;
  RecordID record_id = get<1>(handle);

  // Retrieve the page from the block provided by handle
  SlottedPage* page = file.get(block_id);

  // Retrieve the record from the page
  Dbt* record = page->get(record_id);

  // Unmarshal data for returning
  ValueDict* returnVal = unmarshal(record);

  // Free Memory
  delete record;
  delete page;

  return returnVal;
}

/*
 *  Extracts specific fields from a row handle.
 */
ValueDict* HeapTable::project(Handle handle, const ColumnNames* column_names)
{
  // Get block id from handle
  BlockID block_id = get<0>(handle);
  // Get record id from handle;
  RecordID record_id = get<1>(handle);

  // Retrieve the page from the block provided by handle
  SlottedPage* page = file.get(block_id);

  // Retrieve the record from the page
  Dbt* record = page->get(record_id);

  // Unmarshal data for returning
  ValueDict* retrievedVal = unmarshal(record);
  ValueDict* returnVal = new ValueDict;
  for(auto const& column_name: *column_names)
  {
    ValueDict::const_iterator column = retrievedVal->find(column_name);
    Value value = column->second;
    returnVal->insert(pair<Identifier, Value>(column_name, value));

  }

  // Free Memory
  delete record;
  delete page;
  delete retrievedVal;

  return returnVal;
}

// Protected Functions
ValueDict* HeapTable::validate(const ValueDict* row)
{
  ValueDict* fullRow = new ValueDict();
  uint col_num = 0;

  for (auto const& column_name: this->column_names) {
    ColumnAttribute ca = this->column_attributes[col_num++];
    ValueDict::const_iterator column = row->find(column_name);
    Value value = column->second;

    // If data type not recognized throw error, else push it
    if (ca.get_data_type() != ColumnAttribute::DataType::INT && ca.get_data_type()
    != ColumnAttribute::DataType::TEXT) {
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }
    else
    {
      fullRow->insert(pair<Identifier, Value>(column_name, value));
    }
  }

  return fullRow;
}

Handle HeapTable::append(const ValueDict* row)
{
  Dbt* data = marshal(row);
  SlottedPage* block = file.get(file.get_last_block_id());
  RecordID record_id;

  try
  {
    // Try to add block
    record_id = block->add(data);
  }
  // block is full
  catch(DbBlockNoRoomError e)
  {
    // clear memory
    delete block;

    // Make a new block and add the data
    block = file.get_new();
    record_id = block->add(data);
  }

  // Commit changes
  file.put(block);

  Handle retHandle = pair<BlockID, RecordID>(file.get_last_block_id(), record_id);
  return retHandle;
}

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt* HeapTable::marshal(const ValueDict* row) {
    // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    char *bytes = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;
    for (auto const& column_name: this->column_names) {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            *(int32_t*) (bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            *(u_int16_t*) (bytes + offset) = size;
            offset += sizeof(u_int16_t);
            memcpy(bytes+offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict* HeapTable::unmarshal(Dbt* data)
{
  ValueDict* retData = new ValueDict();
  uint col_num = 0;
  uint offset = 0;

  for (auto const& column_name: this->column_names) {

    ColumnAttribute ca = this->column_attributes[col_num++];
    Value value;

    // retrieve the data, store
    void * dbtData = data->get_data();

    if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
      // Copy memory into container, then put information into value.n and
      // increment the offset
      int32_t *container = new int32_t;
      memcpy(container, (const int32_t *) dbtData + offset, sizeof(int32_t));
      value.n = *container;
      offset += sizeof(int32_t);

      delete container;
    } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
      // Record the length of the string
      u_int16_t length = *(u_int16_t*)((char *)dbtData + offset);

      // Create a container of length of the string
      char *container = new char[length];

      offset += sizeof(u_int16_t);


      // Copy data into container

      memcpy(container, (char*)dbtData + offset, length);
      offset += length;

      // Cast data to a string, store in value s
      value.s = string(container);
      delete [] container;
    } else {
      cout << ca.get_data_type() << endl;
      throw DbRelationError("Only know how to marshal INT and TEXT");
    }

    retData->insert(pair<Identifier, Value>(column_name, value));
  }

  return retData;

}

bool test_heap_storage()
{

  ColumnNames column_names;
column_names.push_back("a");
column_names.push_back("b");
ColumnAttributes column_attributes;
ColumnAttribute ca(ColumnAttribute::INT);
column_attributes.push_back(ca);
ca.set_data_type(ColumnAttribute::TEXT);
column_attributes.push_back(ca);
HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
table1.create();
std::cout << "create ok" << std::endl;
table1.drop();  // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
std::cout << "drop ok" << std::endl;

HeapTable table("_test_data_cpp", column_names, column_attributes);
table.create_if_not_exists();
std::cout << "create_if_not_exsts ok" << std::endl;

ValueDict row;
row["a"] = Value(12);
row["b"] = Value("Hello!");
std::cout << "try insert" << std::endl;
table.insert(&row);
std::cout << "insert ok" << std::endl;
Handles* handles = table.select();
std::cout << "select ok " << handles->size() << std::endl;
ValueDict *result = table.project((*handles)[0]);
std::cout << "project ok" << std::endl;
Value value = (*result)["a"];

if (value.n != 12)
 return false;
value = (*result)["b"];

if (value.s != "Hello!")
  return false;
table.drop();

return true;
}
