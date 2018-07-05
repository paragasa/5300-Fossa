/* heap_storage.cpp
* Jared Mead, John Nguyen
*/

#include "db_cxx.h";
#include "heap_storage.h";
using namespace std;

typedef u_int16_t RecordID;
typedef vector<RecordID> RecordIDs;

class SlottedPage : public DbBlock {
public:
  //constructor
  SlottedPage(Dbt &block, BlockID block_id, bool is_new=false)
  {
    //if this is a new block
    if(is_new)
    {
      //Intialize protected variables
      this->num_records = 0;
      this->end_free  = DbBlcok::BLOCK_SZ -1;
      //default header creation
      put_header();
    }
    //if the block was already initialized return state information via header
    else
    {
      get_header(this->num_records),this->end_free);
    }

  }
	// Big 5 - we only need the destructor, copy-ctor, move-ctor, and op= are unnecessary
	// but we delete them explicitly just to make sure we don't use them accidentally

  //destructor
  virtual ~SlottedPage() {}
  // not sure what this stuff does yet
	SlottedPage(const SlottedPage& other) = delete;
	SlottedPage(SlottedPage&& temp) = delete;
	SlottedPage& operator=(const SlottedPage& other) = delete;
	SlottedPage& operator=(SlottedPage& temp) = delete;
  //

  //adds data as a new record to the slotted page block
	RecordID add(const Dbt* data) throw(DbBlockNoRoomError)
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

    return id
  }
  // Takes in a record id, need to check the record size and return the data
	Dbt* get(RecordID record_id)
  {

      //retrieve header
      u_int16_t size;
      u_int16_t loc;
      get_header(size,loc,record_id);
      //if the location is zero this means the record is deleted
      if(!loc == 0)
      {
        //construct berkeleyDB:Dbt with data
        return Dbt((void*) loc, size);
      }
      else
      {
        //need to do something if its deleted
      }
  }

  //updates a record using an ID and data
	void put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError)
  {
    u_int16_t size;
    u_int16_t loc;
    get_header(size,loc,record_id);
    //can we only insert same sized data wtihout having to move things
    //Smaller data would fit I guess but that would leave empty space
    if(data->get_size() != size)
    {
      throw DbBlockNoRoomError("not enough room to add data");
    }

    //set the header at the end
    put_header(record_id,size,loc);
    // updates block with the data
    memcpy(this->address(loc),data->get_data(),size);

  }

	void del(RecordID record_id)
  {
    u_int16_t size;
    u_int16_t loc;
    get_header(size,loc,record_id);
    put_header(record_id,0 ,0);
    //compact the records space
    slide(loc, loc + size);
  }

  //returns only the non deleted pins
	RecordIDs* ids()
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
        results.push_back(id);
      }
    }
    //return the pointer to the vector
    return results;
  }

protected:
	u_int16_t num_records;
	u_int16_t end_free;

  //puts an integer n at the specified offset
  void put_n(u_int16_t offset, u_int16_t n)
  {
    *(u_int16_t*)this->address(offset) = n;
  }

  //gets an integer n at the specified offset
  u_int16_t get_n(u_int16_t offset)
  {
    return *(u_int16_t*)this->address(offset);
  }

  //two returnparameters size and loc
	void get_header(u_int16_t &size, u_int16_t &loc, RecordID id=0)
  {
    size = get_n(4*id);
    loc = get_n(4*id+2);
  }
  //Stores the id and size of header to a location
  //Defaults to store the header of the entire block when using put_header
  void put_header(RecordID id=0, u_int16_t size=0, u_int16_t loc=0){
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
	bool has_room(u_int16_t size)
  {
    //calculates number of records plus header
    available = (end_free - (num_records + 1)) * 4;
    //if size of new data is smaller or equal to available, we can add it
    return (size <= available);
  }

  //Two cases:
  //start< end removes data from start up to end
  //end<start makes room for extra data from end to start
	void slide(u_int16_t start, u_int16_t end)
  {
    u_int16_t shift = end - start;
    if(shift == 0)
      return;
    //slide dataype


    for(u_int16_t id in this->ids())
    {
      u_int16_t size;
      u_int16_t loc;
      get_header(size,loc,record_id);

    }
  }


  //returns a void pointer with address of the data + offset
	void* address(u_int16_t offset)
  {
    return (void*)((char*)this->block.get_data() + offset);
  }

};
