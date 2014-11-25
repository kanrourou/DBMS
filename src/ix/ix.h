#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <cassert>
#include <cmath>
#include <queue>
#include <climits>
#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define EPSILON 1e-7 // determine the equality of two floats

class IXFileHandle;
class IX_ScanIterator;

// key slot
struct KeySlot
{
	void* key;
	unsigned keyLength;
	unsigned numOfRids;
	unsigned ridOffset;
};


class IndexManager {
public:
	static IndexManager* instance();

	// Create index file(s) to manage an index
	RC createFile(const string &fileName, const unsigned &numberOfPages);

	// Delete index file(s)
	RC destroyFile(const string &fileName);

	// Open an index and returns an IXFileHandle
	RC openFile(const string &fileName, IXFileHandle &ixFileHandle);

	// Close an IXFileHandle.
	RC closeFile(IXFileHandle &ixfileHandle);


	// The following functions  are using the following format for the passed key value.
	//  1) data is a concatenation of values of the attributes
	//  2) For INT and REAL: use 4 bytes to store the value;
	//     For VarChar: use 4 bytes to store the length of characters, then store the actual characters.

	// Insert an entry to the given index that is indicated by the given IXFileHandle
	RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// Delete an entry from the given index that is indicated by the given IXFileHandle
	RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

	// scan() returns an iterator to allow the caller to go through the results
	// one by one in the range(lowKey, highKey).
	// For the format of "lowKey" and "highKey", please see insertEntry()
	// If lowKeyInclusive (or highKeyInclusive) is true, then lowKey (or highKey)
	// should be included in the scan
	// If lowKey is null, then the range is -infinity to highKey
	// If highKey is null, then the range is lowKey to +infinity

	// Initialize and IX_ScanIterator to supports a range search
	RC scan(IXFileHandle &ixfileHandle,
			const Attribute &attribute,
			const void        *lowKey,
			const void        *highKey,
			bool        lowKeyInclusive,
			bool        highKeyInclusive,
			IX_ScanIterator &ix_ScanIterator);

	// Generate and return the hash value (unsigned) for the given key
	unsigned hash(const Attribute &attribute, const void *key);


	// Print all index entries in a primary page including associated overflow pages
	// Format should be:
	// Number of total entries in the page (+ overflow pages) : ??
	// primary Page No.??
	// # of entries : ??
	// entries: [xx] [xx] [xx] [xx] [xx] [xx]
	// overflow Page No.?? liked to [primary | overflow] page No.??
	// # of entries : ??
	// entries: [xx] [xx] [xx] [xx] [xx]
	// where [xx] shows each entry.
	RC printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber);

	// Get the number of primary pages
	RC getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages);

	// Get the number of all pages (primary + overflow)
	RC getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages);

protected:
	IndexManager   ();                            // Constructor
	~IndexManager  ();                            // Destructor

private:
	static IndexManager *_index_manager;
	PagedFileManager *pfm;
	unsigned nextPointer;
	unsigned level;
	unsigned baseNumOfPages;
	unsigned numOfIndexPages;
	unsigned numOfOverflowPages;

	//initialize metadata file
	RC initializeMetadataFile(IXFileHandle ixFileHandle, unsigned numOfPages);

	//initialize primary file
	RC initializePrimaryFile(IXFileHandle ixFileHandle, unsigned numOfPages);

	//get bucket according to hashValue
	unsigned getBucket(unsigned hashValue);

	//look for specific key slot in page, true for found, otherwise return false
	bool lookForKeySlotInPage(void *page, const Attribute &attribute, const void *key, unsigned &slotOffset, unsigned &keySlotNum);

	//insert rid at current page
	void insertEntryAtCurrentPage(unsigned keySlotOffset, unsigned keySlotNum, const RID &rid, void *page,
			const Attribute &attribute, const void *key, bool keyExistsInPage);

	//calculate space needed for rid record insertion
	unsigned getEntrySize(const Attribute &attribute, const void *key);

	//append rid and key slot to the page
	void appendRidAndKeySlot(const RID &rid, const Attribute &attribute, const void *key, void *page);

	//insert rid into page
	void insertRidWithSwapping(unsigned keySlotOffset, unsigned keySlotNum, const RID &rid, void *page, const Attribute &attribute, const void *key);

	//get next key slot offset
	//assure currentOffset is correct and there is next slot before call this function
	void getNextKeySlotOffset(const Attribute &attribute, void *page, unsigned currentOffset, unsigned &nextOffset);

	//traversal before inserting to prevent duplicates, find available page at the same time
	RC traversal(IXFileHandle &ixfileHandle, void* startPage, void* availablePage, unsigned entrySize,
			const Attribute &attribute, const void* key, const RID &rid, bool &isValid,
			unsigned &pageNum, unsigned &pageFlag, bool &splitFlag);

	//detect dupliacates
	bool hasDuplicates(void* page, const Attribute &attribute, const void* key, const RID &rid);

	//if the page is available for the entry
	bool isPageAvailable(void *page, unsigned entrySize);

	//look for page in index
	RC lookForPageInFreePageIndex(IXFileHandle &ixfileHandle, void* page, unsigned &pageNum,
			bool &find, unsigned currIndexPageNum);

	//get pageNum to insert, 0 represent index, 1 represent overflow page
	RC getFreePage(IXFileHandle &ixfileHandle, unsigned &newPageNum, unsigned pageFlag);

	//add page to free page index, 0 represent index, 1 represent overflow page
	RC addFreePage(IXFileHandle &ixfileHandle, unsigned newPageNum, unsigned pageFlag);

	//initialize index page
	void initializeIndexPage(void *page);

	//initialize overflow page
	void initializeOverflowPage(void *page);

	//initialize bucket
	void initializeBucket(void *page);

	//insert free page into index page
	RC insertPageIntoFreePageIndex(IXFileHandle &ixfileHandle, void* page, unsigned newPageNum,
			unsigned currIndexPageNum);

	//split
	RC split(IXFileHandle &ixfileHandle, const Attribute &attribute);

	RC updateNumOfIndexPages(IXFileHandle &ixfileHandle);

	RC updateNumOfOverflowPages(IXFileHandle &ixfileHandle);

	// private method
	void freeMemory(char* page_dummy);
	void readPage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum, char* pageDummy);
	void writePage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum, char* pageDummy);
	RC deleteEntryFromPage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum, const Attribute &attribute,
			const void *key, const RID &rid, unsigned &preFlag, int &prePage, int &nextPage, unsigned &freespaceOffset);
	void deleteEmptyPage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum,
			unsigned preFlag, int prePage, int nextPage);
	bool needMerge(IXFileHandle &ixfileHandle);
	void mergeBucket(IXFileHandle &ixfileHandle);
	void getRidPageInfo(const char* pageDummy, int &nextPage, int &prePage, unsigned &preFlag,
			unsigned &freespaceLength, unsigned &freespaceOffset, unsigned &numOfKeySlot);
	void resolveKey(const Attribute &attribute, const void *key, int &valueInt, float &valueReal,
			unsigned &charLength, char* &valueVChar, string &valueString);
	void findKeySlot(char* pageDummy, const Attribute &attribute, int valueInt, float valueReal, string valueString,
			unsigned numOfKeySlot, unsigned &keySlotPointer,  unsigned &ridOffset, unsigned &numOfRid);
	void coverDeleted(char* pageDummy, const Attribute &attribute, int keySlotPointer, int ridPointer);
	void getCurKeySlotOffset(char* pageDummy, const Attribute &attribute, unsigned preOffset, unsigned &curOffset);
	unsigned hashInt(int key);
	unsigned hashReal(float key);
	unsigned hashVarChar(char* key);
	void getCurKeySlot(char* pageDummy, const Attribute &attribute, unsigned preOffset, unsigned &curOffset, unsigned &ridOffset,
			unsigned &numOfRids, void* &key);
	void getInUseOverflowPages(IXFileHandle &ixfileHandle, queue<unsigned> &overflowPageCache);

};

class IXFileHandle {
public:
	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);
	RC openIndexFiles(const string &fileName, PagedFileManager *pfm);//must be set before using
	RC appendPageToMetadataFile(const void *data);//append page to metadata file
	RC appendPageToPrimaryFile(const void *data);//append page to primary file
	RC readPageFromMetadataFile(PageNum pageNum, void *data);//read page from metadata file
	RC readPageFromPrimaryFile(PageNum pageNum, void *data);//read page from primary file
	RC writePageToMetadataFile(PageNum pageNum, const void *data);//write page to meta file
	RC writePageToPrimaryFile(PageNum pageNum, const void *data);//write page to primary file
	RC closeIndexFiles(PagedFileManager *pfm);//close index files
	unsigned getNumOfPagesInMetadataFile();//get pages in metadata file
	unsigned getNumOfPagesInPrimaryFile();//get pages in primary file

	IXFileHandle();  							// Constructor
	~IXFileHandle(); 							// Destructor

private:
	FileHandle metadataFileHandle;
	FileHandle primaryFileHandle;
	unsigned readPageCounter;
	unsigned writePageCounter;
	unsigned appendPageCounter;

};


class IX_ScanIterator {
 public:
  IX_ScanIterator();  							// Constructor
  ~IX_ScanIterator(); 							// Destructor

  RC getNextEntry(RID &rid, void *key);  		// Get next matching entry
  RC close();             						// Terminate index scan

  inline void setIXFileHandle(IXFileHandle &_ixfileHandle) {
	  ixfileHandle = _ixfileHandle;
  }
  inline void setAttribute(const Attribute &_attribute) {
      attribute.name = _attribute.name;
      attribute.type = _attribute.type;
      attribute.length = _attribute.length;
  }
  inline void setLowKey(const void *_lowKey) {
	  lowKey = _lowKey;
  }
  inline void setHighKey(const void *_highKey) {
	  highKey = _highKey;
  }
  inline void setLowKeyInclusive(bool _lowKeyInclusive) {
	  lowKeyInclusive = _lowKeyInclusive;
  }
  inline void setHighKeyInclusive(bool _highKeyInclusive) {
	  highKeyInclusive = _highKeyInclusive;
  }
  inline void setPrimaryPageCache(unsigned numOfBuckets) {
  	for(int i = 0; i < numOfBuckets; i++) {
  		primaryPageCache.push(i);
  	}
  }
  inline void setOverflowPageCache(queue<unsigned> &_overflowPageCache) {
	  overflowPageCache = _overflowPageCache;
  }
  inline void resetOthers() {
      ridPointer = 0;
      if(pageDummy == NULL) {
          pageDummy = (char*) malloc(PAGE_SIZE);
          assert(pageDummy != NULL);
      }
      // empty current queue
      queue<unsigned> empty1, empty2;
      swap(primaryPageCache, empty1);
      swap(overflowPageCache, empty2);
      (*preRid).pageNum = 0xffffffff;
      (*preRid).slotNum = 0xffffffff;
      isStart = true;
  }


 private:
  // method
  bool isEOF();
  RC iterNewPage();
  void getCurKeySlot(unsigned preOffset, unsigned &curOffset, unsigned &ridOffset, unsigned &numOfRids, void* &key, unsigned &keyLength);
  bool isKeyValid(void* key);
  RC findCurEntry(RID &rid, void* key);
  RC findNextEntry(RID &rid, void* key);


  // obtained form ix_manager.scan()
  IXFileHandle ixfileHandle;
  Attribute attribute;
  const void *lowKey;
  const void *highKey;
  bool lowKeyInclusive;
  bool highKeyInclusive;
  // initialized when calling ix_manager.scan()
  queue<unsigned> primaryPageCache;
  queue<unsigned> overflowPageCache;
  //
  queue<KeySlot*> keySlotList; // contains only valid key entries
  KeySlot *curKeySlot;
  unsigned ridPointer;
  char* pageDummy;
  RID *preRid;
  // indicate whether is start of the iterator
  bool isStart;

};

// print out the error message for a given return code
void IX_PrintError (RC rc);


#endif
