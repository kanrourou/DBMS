#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <set>

#include "../rbf/pfm.h"

using namespace std;
#define SLOT_SIZE 3 * sizeof(int)
#define FREE_SPACE_DIR_SIZE 1024
#define FIELD_INFO_SIZE 2 * sizeof(int)
#define RID_RECORD_SIZE 3 * sizeof(int)


// Record ID
typedef struct
{
	unsigned pageNum;
	unsigned slotNum;
} RID;


bool operator==(const RID &rid1, const RID &rid2);
bool operator!=(const RID &rid1, const RID &rid2);
bool operator<(const RID &rid1, const RID &rid2);
bool operator<=(const RID &rid1, const RID &rid2);
bool operator>(const RID &rid1, const RID &rid2);
bool operator>=(const RID &rid1, const RID &rid2);


// Attribute
typedef enum { TypeInt = 0, TypeReal, TypeVarChar } AttrType;

typedef unsigned AttrLength;

struct Attribute {
	string   name;     // attribute name
	AttrType type;     // attribute type
	AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum { EQ_OP = 0,  // =
	LT_OP,      // <
	GT_OP,      // >
	LE_OP,      // <=
	GE_OP,      // >=
	NE_OP,      // !=
	NO_OP       // no condition
} CompOp;



/****************************************************************************
The scan iterator is NOT required to be implemented for part 1 of the project 
 *****************************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

// RBFM_ScanIterator is an iteratr to go through records
// The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();


class RBFM_ScanIterator {
public:
	RBFM_ScanIterator();
	~RBFM_ScanIterator();

	// "data" follows the same format as RecordBasedFileManager::insertRecord()
	RC getNextRecord(RID &rid, void *data);
	RC close();

	inline void setFileHandle(FileHandle &fileHandle) {
		_file_handle = fileHandle;
	}
	inline void setRecordDescriptor(const vector<Attribute> &recordDescriptor) {
		_record_descriptor = recordDescriptor;
	}
	inline void setConditionAttribute(const string &conditionAttribute) {
		_condition_attribute = conditionAttribute;
	}
	inline void setCompOp(const CompOp compOp) {
		_comp_op = compOp;
	}
	inline void setValue(const void *value) {
		_value = value;
	}
	inline void setAttributeNames(const vector<string> &attributeNames) {
		_attribute_names = attributeNames;
	}
	inline void setPageNumber(FileHandle &fileHandle) {
		_page_number = fileHandle.getNumberOfPages();
	}

	static const int _page_block_size = 1024;

private:

	// method
	bool isEOF();
	RC getNextRecordHelper(RID &rid, void *data, bool &is_qualified);
	void getBlockFromDisk();
	void getSlotNumber();
	void updateSlotPointer();
	RC getRealConditionalRecord(RID &rid, RID &realRid, void* data, bool &is_qualified);
	void filterRecord(char *page_dummy, int record_offset, void *data, bool &is_qualified);
	bool resolveCondition(char* page_dummy, int record_offset);
	bool resolveConditionHelper(char* page_dummy, int record_offset, int attribute_offset, AttrType attr_type);
	void readRequestedAttributes(char *page_dummy, int record_offset, void *data, bool &is_qualifed);
	RC independentGetRequestedAttributes(RID &rid, RID &realRid, void* data, bool &is_qualified);


	// obtained from rbmf.scan()
	FileHandle _file_handle;
	vector<Attribute> _record_descriptor;
	string _condition_attribute;
	CompOp _comp_op;
	const void* _value;
	vector<string> _attribute_names;
	//
	int _page_number; // the total page number of the file
	int _page_block_pointer; // initial 0
	int _page_pointer; // initial _page_block_size to indicate reading the next block
	int _slot_number; // the number of slot in current page
	int _slot_pointer; // initial 0
	char* _page_dummy; // store the pages read from disk
	set<RID> _hash_set; // store the already traversed RID
};


class RecordBasedFileManager
{
public:
	static RecordBasedFileManager* instance();

	RC createFile(const string &fileName);

	RC destroyFile(const string &fileName);

	RC openFile(const string &fileName, FileHandle &fileHandle);

	RC closeFile(FileHandle &fileHandle);

	//  Format of the data passed into the function is the following:
	//  1) data is a concatenation of values of the attributes
	//  2) For int and real: use 4 bytes to store the value;
	//     For varchar: use 4 bytes to store the length of characters, then store the actual characters.
	//  !!!The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute()
	RC insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid);

	RC readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data);

	// This method will be mainly used for debugging/testing
	RC printRecord(const vector<Attribute> &recordDescriptor, const void *data);

	/**************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************
IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) are NOT required to be implemented for part 1 of the project
	 ***************************************************************************************************************************************************************
	 ***************************************************************************************************************************************************************/
	RC deleteRecords(FileHandle &fileHandle);

	RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid);

	// Assume the rid does not change after update
	RC updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid);

	RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data);

	RC reorganizePage(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const unsigned pageNumber);

	// scan returns an iterator to allow the caller to go through the results one by one.
	RC scan(FileHandle &fileHandle,
			const vector<Attribute> &recordDescriptor,
			const string &conditionAttribute,
			const CompOp compOp,                  // comparision type such as "<" and "="
			const void *value,                    // used in the comparison
			const vector<string> &attributeNames, // a list of projected attributes
			RBFM_ScanIterator &rbfm_ScanIterator);


	// Extra credit for part 2 of the project, please ignore for part 1 of the project
public:

	RC reorganizeFile(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor);


protected:
	RecordBasedFileManager();
	~RecordBasedFileManager();

private:
	static RecordBasedFileManager *_rbf_manager;
	PagedFileManager* pfm;

	/*
	 * for record reading
	 */
	RC freeMemory(char* page_dummy); // free memory allocated by malloc()

	RC getRealRecordRid(FileHandle &fileHandle, const RID &rid, RID &realRid, char* data); // get the RID which points to the real record

	RC getRealRecord(const RID &rid, char* page_dummy, char* data); // get the content of real record

	/*
	 * for records deleting
	 */
	RC clearFreespaceDirPage(FileHandle &fileHandle, int page_num); // delete records

	RC clearRecordPage(FileHandle &fileHandle, int page_num); // delete records

	/*
	 * for record deleting
	 */
	RC deleteRecordHelper(FileHandle &fileHandle, const RID &rid, RID &realRid);

	/*
	 * for attribute reading
	 */
	RC getRealRecordAttributeRid(FileHandle &fileHandle, const RID &rid, RID &realRid, int idx, char* data);

	RC getRealRecordAttribute(const RID &rid, char* page_dummy, int idx, char* data);

	/*
	 * for page reorganizing
	 */
	RC compressPage(char* new_page, char* old_page);

	/*
	 * for record updating
	 */
	//recursively find the record along the path and update
	void recursivelyLookingForAndUpdate(FileHandle& fileHandle, const vector<Attribute> &recordDescriptor, const void* data, const RID &rid, int recordSize);

	//insert record at current page
	RC insertRecordAtPage(FileHandle& fileHandle, void* page, const vector<Attribute> &recordDescriptor, const void *data, const RID& rid , int oldRecordLen);

	//insert rid record at current page
	RC insertRidRecordAtPage(FileHandle& fileHandle, void* page, const void *ridRecord, const RID& rid, int oldRecordLen);

	//prepare rid record to be inserted
	void prepareRid(void* ridRecord, const RID &rid);

	/*
	 * for record insertion
	 */
	int getPage(FileHandle &fileHandle, void *page, const vector<Attribute> &recordDescriptor, int recordSize, int& freePageNum);      //must be opened first

	//dir start at 0
	int lookForPageInDir(FileHandle &fileHandle, int recordSize);		//looking for available page in free space directory, if not found return -1

	void prepareRecord(const vector<Attribute> &recordDescriptor, void* appendedData, const void* data);//append record with field info

	int getAvailableSlotOffset(void* page, bool& isNewSlot, int& slotNum);

	/*
	 * inline functions, for multipurpose use
	 */ 
	//page must be initialized before insertion
	inline void initializePage(void* page)   //page must be initialized before insertion
	{
		int size = 0;
		memcpy((char*)page + (PAGE_SIZE - sizeof(int)), &size, sizeof(int));//initialize freespace pointer
		memcpy((char*)page + (PAGE_SIZE - 2 * sizeof(int)), &size, sizeof(int));//initialize num of slots
		return;
	}


	inline int getFreeSpace(void* page)
	{
		int offset = getFreeSpaceOffset(page);
		int numOfSlots = getNumOfSlots(page);
		return PAGE_SIZE - offset - numOfSlots * SLOT_SIZE - 2 * sizeof(int);
	}

	/*
	inline bool isPageAvailable(void* page, int recordSize)
	{
		int freeSpace = getFreeSpace(page);
		int diff = freeSpace - recordSize - SLOT_SIZE;//need space for record and slot
		bool isAvailable = diff >=0 ? true: false;
		return isAvailable;
	}
	 */
	//if the slot is a tomb

	inline bool isRecord(const void* page, int recordOffset)
	{
		int recordFlag = 0;
		memcpy(&recordFlag, (char*)page + recordOffset, sizeof(int));
		return recordFlag;
	}

	inline void getRidInPage(const void* page, int recordOffset, RID& rid)
	{
		int recordFlag = 0;
		memcpy(&recordFlag, (char*)page + recordOffset, sizeof(int));
		if (recordFlag)
			return;
		memcpy(&(rid.pageNum), (char*)page + recordOffset + sizeof(int), sizeof(int));
		memcpy(&(rid.slotNum), (char*)page + recordOffset + 2 * sizeof(int), sizeof(int));
		return;
	}

	inline void createTomb(void* page, int slotNum)
	{
		int slotFlag = 0;
		int slotOffset = getSlotOffset(slotNum);
		memcpy((char*)page + slotOffset, &slotFlag, sizeof(int));
		return;
	}

	inline bool isTomb(void* page, int slotNum)
	{
		int slotFlag = 0;
		int slotOffset = getSlotOffset(slotNum);
		memcpy(&slotFlag, (char*)page + slotOffset, sizeof(int));
		return !slotFlag;
	}

	inline int getFreeSpaceOffset(void* page)
	{
		int pos = 0;
		memcpy(&pos, (char*)page + (PAGE_SIZE - sizeof(int)), sizeof(int));
		return pos;
	}

	inline int getSlotOffset(int slotNum)//slot num starts from 0
	{
		return PAGE_SIZE - (slotNum + 1) * SLOT_SIZE - 2 * sizeof(int);
	}

	inline int getNumOfSlots(void* page)
	{
		int num = 0;
		memcpy(&num, (char*)page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
		return num;
	}

	inline void updateNumOfSlots(void* page, int num)
	{
		memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int), &num, sizeof(int));
		return;
	}

	inline void updateFreeSpaceOffset(void* page, int offset)
	{
		memcpy((char*)page + PAGE_SIZE - sizeof(int), &offset, sizeof(int));
		return;
	}

	//get recordOffset and recordLen according to slotNum
	inline void getSlot(int& recordOffset, int& recordLen, int slotNum, void* page)
	{
		int slotOffset = getSlotOffset(slotNum);
		memcpy(&recordOffset, (char*)page + slotOffset + sizeof(int), sizeof(int));
		memcpy(&recordLen, (char*)page + slotOffset + 2 * sizeof(int), sizeof(int));
		return;
	}

	//set slot
	inline void setSlot(void* page, int slotNum, int slotFlag, int recordOffset, int recordLen)
	{
		int slotOffset = getSlotOffset(slotNum);
		memcpy((char*)page + slotOffset, &slotFlag, sizeof(int));//set flag
		memcpy((char*)page + slotOffset + sizeof(int), &recordOffset, sizeof(int));
		memcpy((char*)page + slotOffset + 2 * sizeof(int), &recordLen, sizeof(int));
		return;
	}

};

#endif
