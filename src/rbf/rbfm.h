#ifndef _rbfm_h_
#define _rbfm_h_

#include <string>
#include <vector>
#include <cstdlib>
#include <iostream>

#include "../rbf/pfm.h"

using namespace std;
#define SLOT_SIZE 2 * sizeof(int)


// Record ID
typedef struct
{
  unsigned pageNum;
  unsigned slotNum;
} RID;


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
  RBFM_ScanIterator() {};
  ~RBFM_ScanIterator() {};

  // "data" follows the same format as RecordBasedFileManager::insertRecord()
  RC getNextRecord(RID &rid, void *data) { return RBFM_EOF; };
  RC close() { return -1; };
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

  inline int getPage(FileHandle &fileHandle, void *page, bool& isNew, int recordSize)      //must be opened first
  {
	  //if it is the first page
	  //if we need to append new page, since previous page cannot hold the record
	  if (!isPageAvailable(page, recordSize) || !fileHandle.getNumberOfPages())
	  {
		  fileHandle.appendPage(page);
		  isNew = true;
	  }
	  //get that page
	  return fileHandle.readPage(fileHandle.getNumberOfPages() - 1, page);

  }

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

  inline bool isPageAvailable(void* page, int recordSize)
  {
	  int freeSpace = getFreeSpace(page);
	  int diff = freeSpace - recordSize - SLOT_SIZE;//need space for record and slot
	  bool isAvailable = diff >=0 ? true: false;
	  return isAvailable;
  }

  inline int getFreeSpaceOffset(void* page)
  {
	  int pos = 0;
	  memcpy(&pos, (char*)page + (PAGE_SIZE - sizeof(int)), sizeof(int));
	  return pos;
  }

  inline int getNextSlotOffset(void* page)
  {
	  int numOfSlot = getNumOfSlots(page);
	  return PAGE_SIZE - (numOfSlot + 1) * SLOT_SIZE - 2 * sizeof(int);
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

  inline void getSlot(int& recordOffset, int& recordLen, int slotNum, void* page)
  {
	  int slotOffset = getSlotOffset(slotNum);
	  memcpy(&recordOffset, (char*)page + slotOffset, sizeof(int));
	  memcpy(&recordLen, (char*)page + slotOffset + sizeof(int), sizeof(int));
	  return;
  }
};

#endif
