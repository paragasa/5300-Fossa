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

// not sure what this stuff does yet
//SlottedPage(const SlottedPage& other) = delete;
//SlottedPage(SlottedPage&& temp) = delete;
//SlottedPage& operator=(const SlottedPage& other) = delete;
//SlottedPage& operator=(SlottedPage& temp) = delete;
//

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


// BEGIN HeapTable
/*
 *  Corresponds to CREATE TABLE.
 *
 *  At minimum, it presumably sets up the DbFile and calls its create method.
 *
 *  Throws exception if the table already exists
 */
void HeapTable::create()
{
  if(!isCreated)
  {
    file.create();
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
  file.open();
}

/*
 *  Closes the table, temporarily disabling insert, update, delete, select,
 *  and project methods
 */
void HeapTable::close()
{
  file.close();
}

/*
 * Corresponds to DROP TABLE.
 *
 * Deletes the underlying DbFile.
 */
void HeapTable::drop()
{
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

    if (ca.get_data_type() != ColumnAttribute::DataType::INT || ca.get_data_type()
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
    record_id = block->add(data);
  }
  catch(DbBlockNoRoomError e)
  {
    delete block;
    block = file.get_new();
    record_id = block->add(data);
  }
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

// typedef std::map<Identifier, Value> ValueDict;
ValueDict* HeapTable::unmarshal(Dbt* data)
{
  ValueDict* retData = new ValueDict();
  uint col_num = 0;


  for (auto const& column_name: this->column_names) {

      ColumnAttribute ca = this->column_attributes[col_num++];
      Value value;
      void * dbtData = data->get_data();

      if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
          memcpy(&value, &dbtData, sizeof(int32_t));
      } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
          value =  reinterpret_cast<string &>(dbtData);
      } else {
          throw DbRelationError("Only know how to marshal INT and TEXT");
      }

      retData->insert(pair<Identifier, Value>(column_name, value));
  }

  return retData;

}
