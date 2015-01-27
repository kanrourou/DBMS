
#include "ix.h"
#include <vector>

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
	if(!_index_manager)
		_index_manager = new IndexManager();

	return _index_manager;
}

IndexManager::IndexManager()
{
	baseNumOfPages = 0;
	level = 0;
	nextPointer = 0;
	numOfIndexPages = 0;
	numOfOverflowPages = 0;
	pfm = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName, const unsigned &numberOfPages)
{
	RC rc;
	//name convention
	string metadataFileName = fileName + "_metadata";
	string primaryFileName = fileName + "_primary";
	rc = pfm->createFile(metadataFileName.c_str());
	if (rc) {
		cout << "createFile: error in createFile!" << endl;
		return rc;
	}
	rc = pfm->createFile(primaryFileName.c_str());
	if (rc) {
		cout << "createFile: error in createFile!" << endl;
		return rc;
	}
	IXFileHandle ixFileHandle;
	rc = ixFileHandle.openIndexFiles(fileName, pfm);
	if (rc) {
		cout << "createFile: error in openIndexFile!" << endl;
		return rc;
	}
	rc = initializeMetadataFile(ixFileHandle, numberOfPages);
	if (rc) {
		ixFileHandle.closeIndexFiles(pfm);
		cout << "createFile: error in initializeMetadataFile!" << endl;
		return rc;
	}
	initializePrimaryFile(ixFileHandle, numberOfPages);
	if (rc) {
		ixFileHandle.closeIndexFiles(pfm);
		cout << "createFile: error in initializePrimaryFile!" << endl;
		return rc;
	}
	ixFileHandle.closeIndexFiles(pfm);
	return rc;
}

RC IndexManager::initializeMetadataFile(IXFileHandle ixFileHandle, unsigned numOfPages) {
	RC rc;
	//initialze variables page
	unsigned baseNumOfPages = numOfPages;
	unsigned level = 0;
	unsigned nextPointer = 0;
	unsigned numOfIndexPages = 1;
	unsigned numOfOverflowPages = 0;
	void *page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "initializeMetadataFile: error in malloc!" << endl;
		return -1;
	}
	memcpy((char*)page, &baseNumOfPages, sizeof(unsigned));
	memcpy((char*)page + sizeof(unsigned), &level, sizeof(unsigned));
	memcpy((char*)page + 2 * sizeof(unsigned), &nextPointer, sizeof(unsigned));
	memcpy((char*)page + 3 * sizeof(unsigned), &numOfIndexPages, sizeof(unsigned));
	memcpy((char*)page + 4 * sizeof(unsigned), &numOfOverflowPages, sizeof(unsigned));
	rc = ixFileHandle.appendPageToMetadataFile(page);
	if (rc) {
		cout << "initializeMetadataFile: error in appendPageToMetadataFile!" << endl;
		return rc;
	}
	//initialze free pages directory page
	page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "initializeMetadataFile: error in malloc!" << endl;
		return -1;
	}
	int next = -1;
	int pre = -1;
	unsigned numOfFreePages = 0;
	memcpy((char*)page + PAGE_SIZE - sizeof(int), &next, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int), &pre, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &numOfFreePages, sizeof(unsigned));
	rc = ixFileHandle.appendPageToMetadataFile(page);
	if (rc) {
		cout << "initializeMetadataFile: error in appendPageToMetadataFile!" << endl;
		return rc;
	}
	free(page);
	page = 0;
	return rc;
}

RC IndexManager::initializePrimaryFile(IXFileHandle ixFileHandle, unsigned numOfPages) {
	RC rc;
	void* page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "initializeMetadataFile: error in malloc!" << endl;
		return -1;
	}
	//initialize primary page according to numOfPages
	int next = -1;
	int pre = -1;
	unsigned flag = 0;
	unsigned freeSpaceCapacity = PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned);
	unsigned freeSpaceOffset = 0;
	unsigned numOfKeySlots = 0;
	memcpy((char*)page + PAGE_SIZE - sizeof(int), &next, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int), &pre, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &flag, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), &freeSpaceCapacity, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), &freeSpaceOffset, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), &numOfKeySlots, sizeof(unsigned));
	//append page to primary file according to numOfPages
	for (unsigned i = 0; i < numOfPages; i++) {
		rc = ixFileHandle.appendPageToPrimaryFile(page);
		if (rc) {
			cout << "initializeMetadataFile: error in appendPageToPrimaryFile!" << endl;
			return rc;
		}
	}
	free(page);
	page = 0;
	return rc;
}

RC IndexManager::destroyFile(const string &fileName)
{
	string metadataFileName = fileName + "_metadata";
	string primaryFileName = fileName + "_primary";
	RC rc = pfm->destroyFile(metadataFileName.c_str());
	if (rc) {
		cout << "destroyFile: error in destroyFile!" << endl;
		return rc;
	}
	rc = pfm->destroyFile(primaryFileName.c_str());
	if (rc) {
		cout << "destroyFile: error in destroyFile!" << endl;
		return rc;
	}
	return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixFileHandle)
{
	RC rc = ixFileHandle.openIndexFiles(fileName, pfm);
	if (rc) {
		cout << "openFile: error in openFile!" << endl;
		return rc;
	}
	//initial variables
	void* page = malloc(PAGE_SIZE);
	ixFileHandle.readPageFromMetadataFile(0, page);//get variables page
	memcpy(&baseNumOfPages, page, sizeof(unsigned));//get baseNumOfPages
	memcpy(&level, (char*)page + sizeof(unsigned), sizeof(unsigned));//get level
	memcpy(&nextPointer, (char*)page + 2 * sizeof(unsigned), sizeof(unsigned));//get next
	memcpy(&numOfIndexPages, (char*)page + 3 * sizeof(unsigned), sizeof(unsigned));//get numOfIndexPages
	memcpy(&numOfOverflowPages, (char*)page + 4 * sizeof(unsigned), sizeof(unsigned));//get numOfOverflowPages
	free(page);
	page = 0;
	return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
	baseNumOfPages = 0;
	level = 0;
	nextPointer = 0;
	numOfIndexPages = 0;
	numOfOverflowPages = 0;
	return ixfileHandle.closeIndexFiles(pfm);
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	RC rc;
	unsigned hashCode = hash(attribute, key);
	unsigned bucket = getBucket(hashCode);
	void* page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "insertEntry: error in malloc!" << endl;
		return -1;
	}
	rc = ixfileHandle.readPageFromPrimaryFile(bucket, page);
	if (rc) {
		cout << "insertEntry: error in readPageFromPrimaryFile!" << endl;
		return rc;
	}
	unsigned entrySize = getEntrySize(attribute, key);
	void *availablePage = malloc(PAGE_SIZE);
	if (availablePage == 0) {
		cout << "insertEntry: error in malloc!" << endl;
		return -1;
	}
	bool isValid = true;
	//flag 0 represents bucket, 1 represents overflow page
	unsigned pageNum = bucket, pageFlag = 0;
	bool splitFlag = false;
	rc = traversal(ixfileHandle, page, availablePage, entrySize, attribute, key, rid,
			isValid, pageNum, pageFlag, splitFlag);
	if (rc)
	{
		cout << "insertEntry: error in traversal!" << endl;
		return rc;
	}
	//duplicate <key, rid> pair
	if (!isValid)
	{
		cout << "insertEntry: duplicated <key, rid> pair!" << endl;
		return -1;
	}
	unsigned keySlotOffset = 0;
	unsigned keySlotNum = 0;
	bool keyExistsInPage = lookForKeySlotInPage(availablePage, attribute, key, keySlotOffset, keySlotNum);
	insertEntryAtCurrentPage(keySlotOffset, keySlotNum, rid, availablePage, attribute, key, keyExistsInPage);
	//write to file
	//if bucket
	if (pageFlag == 0)
	{
		rc = ixfileHandle.writePageToPrimaryFile(pageNum, availablePage);
		if (rc)
		{
			cout << "insertEntry: error in writePageToPrimaryFile!" << endl;
			return rc;
		}
	}
	else
	{
		rc = ixfileHandle.writePageToMetadataFile(pageNum, availablePage);
		if (rc)
		{
			cout << "insertEntry: error in writePageToMetadataFile!" << endl;
			return rc;
		}

	}

	//if need to spit
	if (splitFlag)
		split(ixfileHandle, attribute);
	free(page);
	page = 0;
	free(availablePage);
	availablePage = 0;
	return rc;
}

RC IndexManager::split(IXFileHandle &ixfileHandle, const Attribute &attribute)
{
	RC rc = 0;
	//caculate bucket to be splitted
	unsigned oldBucket = nextPointer;
	//update next pointer
	unsigned totalPageOfCurrentLevel = baseNumOfPages * (1 << level);
	bool levelUpdateFlag = false;
	//we need to set next pointer to 0 and update level
	if (nextPointer == totalPageOfCurrentLevel - 1)
	{
		nextPointer = 0;
		level++;
		levelUpdateFlag = true;
	}
	else//do not need to modify level
		nextPointer++;
	void *page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "split: error in malloc!" << endl;
		return -1;
	}
	rc = ixfileHandle.readPageFromMetadataFile(0, page);
	if (rc)
	{
		cout << "split: error in readPageFromMetadataFile!" << endl;
		return rc;
	}
	memcpy((char*)page + 2 * sizeof(unsigned), &nextPointer, sizeof(unsigned));
	//update level if necessary
	if (levelUpdateFlag)
		memcpy((char*)page + sizeof(unsigned), &level, sizeof(unsigned));
	rc = ixfileHandle.writePageToMetadataFile(0, page);
	if (rc)
	{
		cout << "split: error in writePageToMetadataFile!" << endl;
		return rc;
	}
	free(page);
	page = 0;
	//append new bucket
	void *newBucket = malloc(PAGE_SIZE);
	if (newBucket == 0) {
		cout << "split: error in malloc!" << endl;
		return -1;
	}
	initializeBucket(newBucket);
	//append to primary file
	if (oldBucket + totalPageOfCurrentLevel > ixfileHandle.getNumOfPagesInPrimaryFile() - 1)
		rc = ixfileHandle.appendPageToPrimaryFile(newBucket);
	if (rc)
	{
		cout << "split: error in appendPageToPrimaryFile!" << endl;
		return rc;
	}
	//gather bucket and overflow pages
	vector<void*> pageQueue;
	vector<unsigned> pageNumQueue;
	//get bucket
	int next = 0;
	void *bucket = malloc(PAGE_SIZE);
	if (bucket == 0) {
		cout << "split: error in malloc!" << endl;
		return -1;
	}
	rc = ixfileHandle.readPageFromPrimaryFile(oldBucket, bucket);
	if (rc)
	{
		cout << "split: error in readPageFromPrimaryFile!" << endl;
		return rc;
	}
	//add into queue, size of queue will be at least one
	//the first element in queue is bucket
	pageQueue.push_back(bucket);
	pageNumQueue.push_back(oldBucket);
	//update next
	memcpy(&next, (char*)bucket + PAGE_SIZE - sizeof(int), sizeof(int));
	//get over flow pages
	while (next != -1)
	{
		void *overflowPage = malloc(PAGE_SIZE);
		if (overflowPage == 0) {
			cout << "split: error in malloc!" << endl;
			return -1;
		}
		rc = ixfileHandle.readPageFromMetadataFile(next, overflowPage);
		//push to queue
		pageQueue.push_back(overflowPage);
		pageNumQueue.push_back(next);
		//update next
		memcpy(&next, (char*)overflowPage + PAGE_SIZE - sizeof(int), sizeof(int));
	}
	//overwrite oldBucket
	void *overwriteBucket = malloc(PAGE_SIZE);
	if (overwriteBucket == 0) {
		cout << "split: error in malloc!" << endl;
		return -1;
	}
	initializeBucket(overwriteBucket);
	//size of queue will be at least one
	if (pageNumQueue.size() != pageQueue.size())
	{
		cout << "split: pageQueue and pageNumQueue inconsistency" << endl;
		return -1;
	}
	//oldBucket
	unsigned currOverwritePageNum = oldBucket;
	//0 represent bucket, 1 represent overflow page
	unsigned currOverwritePageFlag = 0;
	//new bucket
	unsigned currNewPageNum = oldBucket + totalPageOfCurrentLevel;
	unsigned currNewPageFlag = 0;
	//get old and new bucket num
	unsigned oldBucketNum = oldBucket;
	unsigned newBucketNum = oldBucket + totalPageOfCurrentLevel;
	//for each page
	for (unsigned i = 0; i < pageQueue.size(); ++i)
	{
		void *currPageToBeSplitted = pageQueue[i];
		unsigned keySlotOffset = PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned);
		unsigned numOfKeySlots = 0;
		memcpy(&numOfKeySlots, (char*)currPageToBeSplitted + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), sizeof(unsigned));
		//minus 1 due to the requirement of getNextKeySlot function
		//for each key slot
		for (unsigned j = 0; j < numOfKeySlots; ++j)
		{
			unsigned ridOffset = 0;
			unsigned numOfRids = 0;
			//get rid offset and total num of rids
			memcpy(&ridOffset, (char*)currPageToBeSplitted + keySlotOffset - sizeof(unsigned), sizeof(unsigned));
			memcpy(&numOfRids, (char*)currPageToBeSplitted + keySlotOffset - 2 * sizeof(unsigned), sizeof(unsigned));
			//get keySize for key slot
			unsigned keySize = 0;
			switch(attribute.type)
			{
			case TypeInt:
				keySize = sizeof(int);
				break;
			case TypeReal:
				keySize = sizeof(float);
				break;
			case TypeVarChar:
				unsigned strLen = 0;
				memcpy(&strLen, (char*)currPageToBeSplitted + keySlotOffset - 3 * sizeof(unsigned), sizeof(unsigned));
				keySize = sizeof(unsigned) + strLen;
				break;
			}
			//get the key for key slot
			void *currKey = malloc(keySize);
			if (currKey == 0)
			{
				cout << "split: error in malloc!" << endl;
				return -1;
			}
			switch(attribute.type)
			{
			case TypeInt:
				memcpy(currKey, (char*)currPageToBeSplitted + keySlotOffset - 2 * sizeof(unsigned) - sizeof(int), sizeof(int));
				break;
			case TypeReal:
				memcpy(currKey, (char*)currPageToBeSplitted + keySlotOffset - 2 * sizeof(unsigned) - sizeof(float), sizeof(float));
				break;
			case TypeVarChar:
				unsigned strLen = 0;
				memcpy(&strLen, (char*)currPageToBeSplitted + keySlotOffset - 3 * sizeof(unsigned), sizeof(unsigned));
				memcpy(currKey, &strLen, sizeof(unsigned));
				memcpy((char*)currKey + sizeof(unsigned), (char*)currPageToBeSplitted + keySlotOffset - 3 * sizeof(unsigned) - strLen, strLen);
				break;
			}
			//for each rid
			for (unsigned k =0; k < numOfRids; ++k)
			{
				unsigned currentRidOffset = ridOffset + k * RID_SIZE;
				RID rid = {0, 0};
				//get rid for the specific key
				memcpy(&(rid.pageNum), (char*)currPageToBeSplitted + currentRidOffset, sizeof(unsigned));
				memcpy(&(rid.slotNum), (char*)currPageToBeSplitted + currentRidOffset + sizeof(unsigned), sizeof(unsigned));
				unsigned entrySize = getEntrySize(attribute, currKey);
				unsigned currHashCode = hash(attribute, currKey);
				unsigned currBucketNum = getBucket(currHashCode);
				//which bucket to assign
				if (currBucketNum == oldBucketNum)
				{
					//if current page if full
					if (!isPageAvailable(overwriteBucket, entrySize))
					{	//next pageNum and page buffer
						unsigned pageNumBuffer = 0;
						void *pageBuffer = malloc(PAGE_SIZE);
						if (pageBuffer == 0)
						{
							cout << "split: error in malloc" << endl;
							return -1;
						}
						//get new overflow page
						getFreePage(ixfileHandle, pageNumBuffer, 1);
						rc = ixfileHandle.readPageFromMetadataFile(pageNumBuffer, pageBuffer);
						if (rc)
						{
							cout << "split: error in readPageFromMetadataFile!" << endl;
							return rc;
						}
						//update the next pointer for full page
						memcpy((char*)overwriteBucket + PAGE_SIZE - sizeof(int), &pageNumBuffer, sizeof(int));
						//write the full page to file, two cases, bucket or overflow page
						//if 0, bucket
						if (currOverwritePageFlag == 0)
						{
							rc = ixfileHandle.writePageToPrimaryFile(currOverwritePageNum, overwriteBucket);
							if (rc)
							{
								cout << "split: error in writePageToPrimaryFile!" << endl;
								return rc;
							}
						}
						else//if 1, overflow page
						{
							rc = ixfileHandle.writePageToMetadataFile(currOverwritePageNum, overwriteBucket);
							if (rc)
							{
								cout << "split: error in writePageToMetadataFile!" << endl;
								return rc;
							}
						}

						//update the pre page and pre flag for current page
						memcpy((char*)pageBuffer + PAGE_SIZE - 3 * sizeof(unsigned), &currOverwritePageFlag, sizeof(unsigned));
						memcpy((char*)pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned), &currOverwritePageNum, sizeof(unsigned));
						//get page from buffer
						currOverwritePageNum = pageNumBuffer;
						memcpy(overwriteBucket, pageBuffer, PAGE_SIZE);
						//update pageFlag, appended must be over flow page
						currOverwritePageFlag = 1;
						free(pageBuffer);
						pageBuffer = 0;
					}
					unsigned keySlotOffset = 0;
					unsigned keySlotNum = 0;
					bool keyExistsInPage = lookForKeySlotInPage(overwriteBucket, attribute, currKey, keySlotOffset, keySlotNum);
					insertEntryAtCurrentPage(keySlotOffset, keySlotNum, rid, overwriteBucket, attribute, currKey, keyExistsInPage);

				}
				else if (currBucketNum == newBucketNum)
				{
					//if current page if full
					if (!isPageAvailable(newBucket, entrySize))
					{	//next pageNum and page buffer
						unsigned pageNumBuffer = 0;
						void *pageBuffer = malloc(PAGE_SIZE);
						if (pageBuffer == 0)
						{
							cout << "split: error in malloc" << endl;
							return -1;
						}
						//get new overflow page
						getFreePage(ixfileHandle, pageNumBuffer, 1);
						rc = ixfileHandle.readPageFromMetadataFile(pageNumBuffer, pageBuffer);
						if (rc)
						{
							cout << "split: error in readPageFromMetadataFile!" << endl;
							return rc;
						}
						//update the next pointer for full page
						memcpy((char*)newBucket + PAGE_SIZE - sizeof(int), &pageNumBuffer, sizeof(int));
						//write the full page to file, two cases, bucket or overflow page
						//if 0, bucket
						if (currNewPageFlag == 0)
						{
							rc = ixfileHandle.writePageToPrimaryFile(currNewPageNum, newBucket);
							if (rc)
							{
								cout << "split: error in writePageToPrimaryFile!" << endl;
								return rc;
							}
						}
						else//if 1, overflow page
						{
							rc = ixfileHandle.writePageToMetadataFile(currNewPageNum, newBucket);
							if (rc)
							{
								cout << "split: error in writePageToMetadataFile!" << endl;
								return rc;
							}
						}

						//update the pre page and pre flag for current page
						memcpy((char*)pageBuffer + PAGE_SIZE - 3 * sizeof(unsigned), &currNewPageFlag, sizeof(unsigned));
						memcpy((char*)pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned), &currNewPageNum, sizeof(unsigned));
						//get page from buffer
						currNewPageNum = pageNumBuffer;
						memcpy(newBucket, pageBuffer, PAGE_SIZE);
						//update pageFlag, appended must be over flow page
						currNewPageFlag = 1;
						free(pageBuffer);
						pageBuffer = 0;
					}
					unsigned keySlotOffset = 0;
					unsigned keySlotNum = 0;
					bool keyExistsInPage = lookForKeySlotInPage(newBucket, attribute, currKey, keySlotOffset, keySlotNum);
					insertEntryAtCurrentPage(keySlotOffset, keySlotNum, rid, newBucket, attribute, currKey, keyExistsInPage);
				}
				else
				{
					cout << "split: find wrong bucket!" << endl;
					return -1;
				}
			}
			//update next key slot offset
			getNextKeySlotOffset(attribute, currPageToBeSplitted, keySlotOffset, keySlotOffset);
			free(currKey);
			currKey = 0;
		}
	}
	//write page in the memory back to disk
	//for old bucket
	//if 0, bucket
	if (currOverwritePageFlag == 0)
	{
		rc = ixfileHandle.writePageToPrimaryFile(currOverwritePageNum, overwriteBucket);
		if (rc)
		{
			cout << "split: error in writePageToPrimaryFile!" << endl;
			return rc;
		}
	}
	else//if 1, overflow page
	{
		rc = ixfileHandle.writePageToMetadataFile(currOverwritePageNum, overwriteBucket);
		if (rc)
		{
			cout << "split: error in writePageToMetadataFile!" << endl;
			return rc;
		}
	}

	//for new bucket
	//if 0, bucket
	if (currNewPageFlag == 0)
	{
		rc = ixfileHandle.writePageToPrimaryFile(currNewPageNum, newBucket);
		if (rc)
		{
			cout << "split: error in writePageToPrimaryFile!" << endl;
			return rc;
		}
	}
	else//if 1, overflow page
	{
		rc = ixfileHandle.writePageToMetadataFile(currNewPageNum, newBucket);
		if (rc)
		{
			cout << "split: error in writePageToMetadataFile!" << endl;
			return rc;
		}
	}
	//add free pages to free page index
	for (unsigned i = 0; i < pageNumQueue.size(); ++i)
	{
		//skip the bucket
		if (i != 0)
			addFreePage(ixfileHandle, pageNumQueue[i], 1);
		//free allocated memory
		free(pageQueue[i]);
		pageQueue[i] = 0;
	}
	pageNumQueue.clear();
	pageQueue.clear();
	free(overwriteBucket);
	free(newBucket);
	overwriteBucket = 0;
	newBucket = 0;
	return rc;
}

RC IndexManager::addFreePage(IXFileHandle &ixfileHandle, unsigned newPageNum, unsigned pageFlag)
{
	RC rc;
	//open freePageIndex
	void *page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "addFreePage: error in malloc!" << endl;
		return -1;
	}
	int next = 1;
	unsigned currIndexPageNum = 0;
	//find the last page of index
	while (next != -1)
	{
		currIndexPageNum = next;
		ixfileHandle.readPageFromMetadataFile(next, page);
		memcpy(&next, (char*)page + PAGE_SIZE - sizeof(int), sizeof(int));
	}
	rc = insertPageIntoFreePageIndex(ixfileHandle, page, newPageNum, currIndexPageNum);
	if (rc)
	{
		cout << "addFreePage: error in insertPageIntoFreePageIndex" << endl;
		return rc;
	}
	//update num of index pages or num of overflow pages
	if (pageFlag == 0)
	{
		numOfIndexPages--;
		rc = updateNumOfIndexPages(ixfileHandle);
		if (rc)
		{
			cout << "addFreePage: error in updateNumOfIndexPages" << endl;
			return rc;
		}
	}
	else
	{
		numOfOverflowPages--;
		rc = updateNumOfOverflowPages(ixfileHandle);
		if (rc)
		{
			cout << "addFreePage: error in updateNumOfOverflowPages" << endl;
			return rc;
		}
	}
	free(page);
	page = 0;
	return rc;
}

RC IndexManager::insertPageIntoFreePageIndex(IXFileHandle &ixfileHandle, void* page,
		unsigned newPageNum, unsigned currIndexPageNum)
{
	RC rc = 0;
	unsigned numOfFreePages = 0;
	memcpy(&numOfFreePages, (char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), sizeof(unsigned));
	unsigned freeSpaceCapacity = PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned) - numOfFreePages * sizeof(unsigned);
	//if there is space, insert to that page and update index file
	if (freeSpaceCapacity >= sizeof(unsigned))
	{
		//insert free page
		unsigned offset = numOfFreePages * sizeof(unsigned);
		memcpy((char*)page + offset, &newPageNum, sizeof(unsigned));
		//update num of free pages
		numOfFreePages++;
		memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &numOfFreePages, sizeof(unsigned));
		ixfileHandle.writePageToMetadataFile(currIndexPageNum, page);
		if (rc)
		{
			cout << "insertPageIntoFreePageIndex: error in writePageToMetadataFile" << endl;
			return rc;
		}
	}
	else//if no space, get new free page, concatenate two page, update index file
	{
		unsigned newIndexPageNum = 0;
		//get new free page initialize as index page
		getFreePage(ixfileHandle, newIndexPageNum, 0);
		void *newPage = malloc(PAGE_SIZE);
		if (newPage == 0) {
			cout << "insertEntry: error in malloc!" << endl;
			return -1;
		}
		ixfileHandle.readPageFromMetadataFile(newIndexPageNum, newPage);
		if (rc)
		{
			cout << "insertPageIntoFreePageIndex: error in readPageFromMetadataFile" << endl;
			return rc;
		}
		//update next poiter for previous page
		memcpy((char*)page + PAGE_SIZE - sizeof(int), &newIndexPageNum, sizeof(int));
		//insert to new page
		memcpy((char*)newPage, &newPageNum, sizeof(unsigned));
		//update pre pointer
		memcpy((char*)newPage + PAGE_SIZE - 2 * sizeof(int), &currIndexPageNum, sizeof(int));
		//update num of entry
		unsigned numOfFreePages = 1;
		memcpy((char*)newPage + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &numOfFreePages, sizeof(unsigned));
		//write to file
		ixfileHandle.writePageToMetadataFile(currIndexPageNum, page);
		if (rc)
		{
			cout << "insertPageIntoFreePageIndex: error in writePageToMetadataFile" << endl;
			return rc;
		}
		ixfileHandle.writePageToMetadataFile(newIndexPageNum, newPage);
		if (rc)
		{
			cout << "insertPageIntoFreePageIndex: error in writePageToMetadataFile" << endl;
			return rc;
		}
		free(newPage);
		newPage = 0;
	}
	return rc;
}

RC IndexManager::getFreePage(IXFileHandle &ixfileHandle, unsigned &newPageNum, unsigned pageFlag)
{
	RC rc;
	//open freePageIndex
	void *page = malloc(PAGE_SIZE);
	if (page == 0) {
		cout << "insertEntry: error in malloc!" << endl;
		return -1;
	}
	int next = 1;
	unsigned currIndexPageNum = 0;
	//find the last page of index
	while (next != -1)
	{
		currIndexPageNum = next;
		rc = ixfileHandle.readPageFromMetadataFile(next, page);
		if (rc)
		{
			cout << "getFreePage: error in readPageFromMetadataFile" << endl;
			return rc;
		}
		memcpy(&next, (char*)page + PAGE_SIZE - sizeof(int), sizeof(int));
	}
	unsigned freePageNum = 0;
	bool find = false;
	lookForPageInFreePageIndex(ixfileHandle, page, freePageNum, find, currIndexPageNum);
	//if find reset content
	if (find)
	{
		void *resetPage = malloc(PAGE_SIZE);
		ixfileHandle.readPageFromMetadataFile(freePageNum, resetPage);
		if (pageFlag == 0)
			initializeIndexPage(resetPage);
		else
			initializeOverflowPage(resetPage);
		ixfileHandle.writePageToMetadataFile(freePageNum, resetPage);
	}

	//if no free page, append new page
	if (!find)
	{
		void *newPage = malloc(PAGE_SIZE);
		if (newPage == 0) {
			cout << "insertEntry: error in malloc!" << endl;
			return -1;
		}
		//if index page
		if (pageFlag == 0)
			initializeIndexPage(newPage);
		else
			initializeOverflowPage(newPage);
		rc = ixfileHandle.appendPageToMetadataFile(newPage);
		if (rc)
		{
			cout << "getFreePage: error in appendPageToMetadataFile" << endl;
			return rc;
		}
		//update num of index pages or num of overflow pages
		if (pageFlag == 0)
		{
			numOfIndexPages++;
			rc = updateNumOfIndexPages(ixfileHandle);
			if (rc)
			{
				cout << "getFreePage: error in updateNumOfIndexPages" << endl;
				return rc;
			}
		}
		else
		{
			numOfOverflowPages++;
			rc = updateNumOfOverflowPages(ixfileHandle);
			if (rc)
			{
				cout << "getFreePage: error in updateNumOfOverflowPages" << endl;
				return rc;
			}
		}
		freePageNum = ixfileHandle.getNumOfPagesInMetadataFile() - 1;
		free(newPage);
		newPage = 0;
	}
	newPageNum = freePageNum;
	free(page);
	page = 0;
	return rc;
}

void IndexManager::initializeIndexPage(void *page)
{
	int next = -1;
	int pre = -1;
	int numOfFreePages = 0;
	memcpy((char*)page + PAGE_SIZE - sizeof(int), &next, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int), &pre, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &numOfFreePages, sizeof(unsigned));
}

void IndexManager::initializeOverflowPage(void *page)
{
	int next = -1;
	int pre = -1;
	unsigned flag = 1;
	unsigned freeSpaceCapacity = PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned);
	unsigned freeSpaceOffset = 0;
	unsigned numOfKeySlots = 0;
	memcpy((char*)page + PAGE_SIZE - sizeof(int), &next, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int), &pre, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &flag, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), &freeSpaceCapacity, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), &freeSpaceOffset, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), &numOfKeySlots, sizeof(unsigned));
}

void IndexManager::initializeBucket(void *page)
{
	int next = -1;
	int pre = -1;
	unsigned flag = 0;
	unsigned freeSpaceCapacity = PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned);
	unsigned freeSpaceOffset = 0;
	unsigned numOfKeySlots = 0;
	memcpy((char*)page + PAGE_SIZE - sizeof(int), &next, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int), &pre, sizeof(int));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &flag, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), &freeSpaceCapacity, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), &freeSpaceOffset, sizeof(unsigned));
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), &numOfKeySlots, sizeof(unsigned));
}

RC IndexManager::updateNumOfIndexPages(IXFileHandle &ixfileHandle)
{
	void *page = malloc(PAGE_SIZE);
	if (page == 0)
	{
		cout<< "updateNumOfIndexPages: error in malloc" << endl;
		return -1;
	}
	RC rc = ixfileHandle.readPageFromMetadataFile(0, page);
	if (rc)
	{
		cout<< "updateNumOfIndexPages: error in readPageFromMetadataFile" << endl;
		return rc;
	}
	memcpy((char*)page + 3 * sizeof(unsigned), &numOfIndexPages, sizeof(unsigned));
	rc = ixfileHandle.writePageToMetadataFile(0, page);
	if (rc)
	{
		cout<< "updateNumOfIndexPages: error in writePageToMetadataFile" << endl;
		return rc;
	}
	return rc;
}

RC IndexManager::updateNumOfOverflowPages(IXFileHandle &ixfileHandle)
{
	void *page = malloc(PAGE_SIZE);
	if (page == 0)
	{
		cout<< "updateNumOfOverflowPages: error in malloc" << endl;
		return -1;
	}
	RC rc = ixfileHandle.readPageFromMetadataFile(0, page);
	if (rc)
	{
		cout<< "updateNumOfOverflowPages: error in readPageFromMetadataFile" << endl;
		return rc;
	}
	memcpy((char*)page + 4 * sizeof(unsigned), &numOfOverflowPages, sizeof(unsigned));
	rc = ixfileHandle.writePageToMetadataFile(0, page);
	if (rc)
	{
		cout<< "updateNumOfOverflowPages: error in writePageToMetadataFile" << endl;
		return rc;
	}
	return rc;
}

RC IndexManager::lookForPageInFreePageIndex(IXFileHandle &ixfileHandle, void* page, unsigned &pageNum,
		bool &find, unsigned currIndexPageNum)
{
	RC rc = 0;
	unsigned numOfFreePages = 0;
	//find last entry in page
	memcpy(&numOfFreePages, (char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), sizeof(unsigned));
	//if index is empty
	if (numOfFreePages == 0)
	{
		find = false;
		return 0;
	}
	unsigned offset = sizeof(unsigned) * (numOfFreePages - 1);
	//get free page num
	memcpy(&pageNum, (char*)page + offset, sizeof(unsigned));
	find = true;
	//update num of free pages
	numOfFreePages--;
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &numOfFreePages, sizeof(unsigned));
	ixfileHandle.writePageToMetadataFile(currIndexPageNum, page);
	//if no entry in this page
	//delete the page
	if (numOfFreePages == 0)
	{
		int pre = -1;
		memcpy(&pre, (char*)page + PAGE_SIZE - 2 * sizeof(int), sizeof(int));
		//if is not the head of list, mark them as free page, update free page index
		if (pre != -1)
		{
			void *prePage = malloc(PAGE_SIZE);
			if (prePage == 0)
			{
				cout << "insertEntry: error in malloc!" << endl;
				return -1;
			}
			rc = ixfileHandle.readPageFromMetadataFile(pre, prePage);
			if (rc)
			{
				cout << "lookForPageInFreePageIndex: error in readPageFromMetadataFile" << endl;
				return rc;
			}
			//cut linked list at the page before the page to be deleted
			int next = -1;
			memcpy((char*)prePage + PAGE_SIZE - sizeof(int), &next, sizeof(int));
			ixfileHandle.writePageToMetadataFile(pre, prePage);
			if (rc)
			{
				cout << "lookForPageInFreePageIndex: error in writePageToMetadataFile" << endl;
				return rc;
			}
			//reset content of currpage
			initializeIndexPage(page);
			rc = ixfileHandle.writePageToMetadataFile(currIndexPageNum, page);
			if (rc)
			{
				cout << "lookForPageInFreePageIndex: error in writePageToMetadataFile" << endl;
				return rc;
			}
			//make sure cut list before update free page index, otherwise there will be bug
			addFreePage(ixfileHandle, currIndexPageNum, 0);
			free(prePage);
			prePage = 0;
		}
	}
	return rc;
}

RC IndexManager::traversal(IXFileHandle &ixfileHandle, void * startPage, void *availablePage,
		unsigned entrySize, const Attribute &attribute, const void *key, const RID &rid, bool &isValid,
		unsigned &pageNum, unsigned &pageFlag, bool &splitFlag)
{
	RC rc = 0;
	//handle bucket page
	int next = 0;
	bool findAvailable = false;
	memcpy(&next, (char*)startPage + PAGE_SIZE - sizeof(int), sizeof(int));
	if (hasDuplicates(startPage, attribute, key, rid))
	{
		isValid = false;
		return rc;
	}
	//if bucket is available
	if (isPageAvailable(startPage, entrySize))
	{
		findAvailable = true;
		memcpy(availablePage, startPage, PAGE_SIZE);
		//availablePage = startPage;
	}
	//handle overflow pages
	unsigned prePageNum = pageNum;
	unsigned preFlag = 0;
	while (next != -1)
	{
		prePageNum = next;
		preFlag = 1;
		rc = ixfileHandle.readPageFromMetadataFile(next, startPage);
		if (rc)
		{
			cout << "traversal: error in readPageFromMetadataFile" << endl;
			return rc;
		}
		if (hasDuplicates(startPage, attribute, key, rid))
		{
			isValid = false;
			return rc;
		}
		//if find available page
		if (isPageAvailable(startPage, entrySize) && !findAvailable)
		{
			pageNum = next;
			pageFlag = 1;
			findAvailable = true;
			memcpy(availablePage, startPage, PAGE_SIZE);
			//availablePage = startPage;
		}
		memcpy(&next, (char*)startPage + PAGE_SIZE - sizeof(int), sizeof(int));
	}
	if (!findAvailable)
	{
		splitFlag = true;
		//get new free page num
		getFreePage(ixfileHandle, pageNum, 1);
		//update next pointer of prepage
		memcpy((char*)startPage + PAGE_SIZE - sizeof(int), &pageNum, sizeof(unsigned));
		//pre page is bucket or overflow page
		if (preFlag == 0)
		{
			rc = ixfileHandle.writePageToPrimaryFile(prePageNum, startPage);
			if (rc)
			{
				cout << "traversal: error in writePageToPrimaryFile" << endl;
				return rc;
			}
		}
		else
		{
			rc = ixfileHandle.writePageToMetadataFile(prePageNum, startPage);
			if (rc)
			{
				cout << "traversal: error in writePageToMetadataFile" << endl;
				return rc;
			}
		}

		//set pre pointer on current page, don't write back now
		rc = ixfileHandle.readPageFromMetadataFile(pageNum, availablePage);
		if (rc)
		{
			cout << "traversal: error in readPageFromMetadataFile" << endl;
			return rc;
		}
		//set prePageNum
		memcpy((char*)availablePage + PAGE_SIZE - 2 * sizeof(int), &prePageNum, sizeof(int));
		//set prePageFlag
		memcpy((char*)availablePage + PAGE_SIZE - 2 * sizeof(int) - sizeof(unsigned), &preFlag, sizeof(unsigned));
		pageFlag = 1;
	}
	return rc;
}

bool IndexManager::hasDuplicates(void* page, const Attribute &attribute, const void *key,
		const RID &rid)
{
	unsigned numOfKeySlots = 0;
	memcpy(&numOfKeySlots, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), sizeof(unsigned));
	unsigned keySlotOffset = 0;
	unsigned keySlotNum = 0;
	bool keyExistsInPage = lookForKeySlotInPage(page, attribute, key, keySlotOffset, keySlotNum);
	if (!keyExistsInPage)
		return false;
	else
	{
		unsigned ridOffset = 0;
		unsigned numOfRids = 0;
		memcpy(&ridOffset, (char*)page + keySlotOffset - sizeof(unsigned), sizeof(unsigned));
		memcpy(&numOfRids, (char*)page + keySlotOffset - 2 * sizeof(unsigned), sizeof(unsigned));
		int counter = 0;
		while (counter < numOfRids)
		{
			unsigned pageNum = 0;
			unsigned slotNum = 0;
			memcpy(&pageNum, (char*)page + ridOffset, sizeof(unsigned));
			memcpy(&slotNum, (char*)page + ridOffset + sizeof(unsigned), sizeof(unsigned));
			if (pageNum == rid.pageNum && slotNum == rid.slotNum)
				return true;
			ridOffset += RID_SIZE;
			counter++;
		}
		return false;
	}
}

bool IndexManager::isPageAvailable(void *page, unsigned entrySize)
{
	unsigned freeSpaceCapacity = 0;
	memcpy(&freeSpaceCapacity, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), sizeof(unsigned));
	if (freeSpaceCapacity >= entrySize)
		return true;
	else
		return false;

}

void IndexManager::insertEntryAtCurrentPage(unsigned keySlotOffset, unsigned keySlotNum,
		const RID &rid, void *page, const Attribute &attribute, const void *key, bool keyExistsInPage)
{
	if(!keyExistsInPage)
		appendRidAndKeySlot(rid, attribute, key, page);
	else
		insertRidWithSwapping(keySlotOffset, keySlotNum, rid, page, attribute, key);
	return;
}

void IndexManager::insertRidWithSwapping(unsigned keySlotOffset, unsigned keySlotNum, const RID &rid, void *page, const Attribute &attribute, const void *key)
{
	unsigned numOfKeySlots = 0;
	memcpy(&numOfKeySlots, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), sizeof(unsigned));
	RID swappingRidBuffer = {rid.pageNum, rid.slotNum};
	unsigned currentKeySlotOffset = keySlotOffset;
	//works even if there is slot with 0 rid
	for (unsigned i = keySlotNum; i < numOfKeySlots; ++i)
	{
		unsigned numOfRids = 0;
		memcpy(&numOfRids, (char*)page + currentKeySlotOffset - 2 * sizeof(unsigned), sizeof(unsigned));
		//if it is an empty key slot, we go to the next slot, skip it
		if (numOfRids == 0)
		{
			getNextKeySlotOffset(attribute, page, currentKeySlotOffset, currentKeySlotOffset);
			continue;
		}
		unsigned ridOffset = 0;
		memcpy(&ridOffset, (char*)page + currentKeySlotOffset - sizeof(unsigned), sizeof(unsigned));
		ridOffset += RID_SIZE * numOfRids;
		//update swapping buffer
		RID tempBuffer = {swappingRidBuffer.pageNum, swappingRidBuffer.slotNum};
		memcpy(&(swappingRidBuffer.pageNum), (char*)page + ridOffset, sizeof(RID_SIZE));
		memcpy(&(swappingRidBuffer.slotNum), (char*)page + ridOffset + sizeof(unsigned), sizeof(RID_SIZE));
		//overwrite stored area
		memcpy((char*)page + ridOffset, &(tempBuffer.pageNum), sizeof(unsigned));
		memcpy((char*)page + ridOffset + sizeof(unsigned), &(tempBuffer.slotNum), sizeof(unsigned));
		//update current keyslot numOfRids if necessary
		if (i == keySlotNum)
		{
			numOfRids++;
			memcpy((char*)page + currentKeySlotOffset - 2 * sizeof(unsigned), &numOfRids, sizeof(unsigned));
		}
		//get next slot offset
		getNextKeySlotOffset(attribute, page, currentKeySlotOffset, currentKeySlotOffset);
		//		//update next slot rid offset
		//		ridOffset += RID_SIZE;
		//		memcpy((char*)page + currentKeySlotOffset - sizeof(unsigned), &ridOffset, sizeof(unsigned));
		//		//update next slot rid num, temporary minus 1
		//		memcpy(&numOfRids, (char*)page + currentKeySlotOffset - 2 * sizeof(unsigned), sizeof(unsigned));
		//		numOfRids--;
		//		memcpy((char*)page + currentKeySlotOffset - 2 * sizeof(unsigned), &numOfRids, sizeof(unsigned));
	}
	//	//one more slot to modify
	//	unsigned numOfRids = 0;
	//	memcpy(&numOfRids, (char*)page + currentKeySlotOffset - 2 * sizeof(unsigned), sizeof(unsigned));
	//	if (numOfRids != 0)
	//	{
	//		unsigned ridOffset = 0;
	//		memcpy(&ridOffset, (char*)page + currentKeySlotOffset - sizeof(unsigned), sizeof(unsigned));
	//		ridOffset += RID_SIZE * numOfRids;
	//		//append to the tail
	//		memcpy((char*)page + ridOffset, &(swappingRidBuffer.pageNum), sizeof(unsigned));
	//		memcpy((char*)page + ridOffset + sizeof(unsigned), &(swappingRidBuffer.slotNum), sizeof(unsigned));
	//	}

	//update page info
	//update freeSpace offset
	unsigned freeSpaceOffset = 0;
	memcpy(&freeSpaceOffset, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), sizeof(unsigned));
	freeSpaceOffset += RID_SIZE;
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), &freeSpaceOffset, sizeof(unsigned));
	//update freeSpaceCapacity
	unsigned freeSpaceCapacity = 0;
	memcpy(&freeSpaceCapacity, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), sizeof(unsigned));
	freeSpaceCapacity -= RID_SIZE;
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), &freeSpaceCapacity, sizeof(unsigned));

	//update the rid offset of all slots after the modified slot, if necessary
	if (keySlotNum == numOfKeySlots - 1)
		return;
	currentKeySlotOffset = keySlotOffset;
	getNextKeySlotOffset(attribute, page, currentKeySlotOffset, currentKeySlotOffset);
	for (unsigned i = keySlotNum + 1; i < numOfKeySlots; ++i)
	{
		unsigned ridOffset = 0;
		memcpy(&ridOffset, (char*)page + currentKeySlotOffset - sizeof(unsigned), sizeof(unsigned));
		ridOffset += RID_SIZE;
		memcpy((char*)page + currentKeySlotOffset - sizeof(unsigned), &ridOffset, sizeof(unsigned));
		getNextKeySlotOffset(attribute, page, currentKeySlotOffset, currentKeySlotOffset);
	}
	//	//one more slot to modify
	//	unsigned ridOffset = 0;
	//	memcpy(&ridOffset, (char*)page + currentKeySlotOffset - sizeof(unsigned), sizeof(unsigned));
	//	ridOffset += RID_SIZE;
	//	memcpy((char*)page + currentKeySlotOffset - sizeof(unsigned), &ridOffset, sizeof(unsigned));

	return;
}

void IndexManager::getNextKeySlotOffset(const Attribute &attribute, void *page,
		unsigned currentOffset, unsigned &nextOffset)
{
	currentOffset -= (2 * sizeof(unsigned));
	switch(attribute.type)
	{
	case TypeInt:
		currentOffset -= sizeof(int);
		break;
	case TypeReal:
		currentOffset -= sizeof(float);
		break;
	case TypeVarChar:
		currentOffset -= sizeof(unsigned);
		unsigned strLen = 0;
		memcpy(&strLen, (char*)page + currentOffset, sizeof(unsigned));
		currentOffset -= strLen;
		break;
	}
	nextOffset = currentOffset;
	return;
}

void IndexManager::appendRidAndKeySlot(const RID &rid, const Attribute &attribute, const void *key, void *page)
{
	unsigned freeSpaceOffset = 0;
	unsigned freeSpaceCapacity = 0;
	memcpy(&freeSpaceOffset, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), sizeof(unsigned));
	memcpy(&freeSpaceCapacity, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), sizeof(unsigned));
	//insert rid
	memcpy((char*)page + freeSpaceOffset, &(rid.pageNum), sizeof(unsigned));
	memcpy((char*)page + freeSpaceOffset + sizeof(unsigned), &(rid.slotNum), sizeof(unsigned));
	//insert keyslot
	unsigned newSlotOffset = freeSpaceOffset + freeSpaceCapacity;//next byte to the right of new slot
	//insert keyslot offset
	unsigned numOfRids = 1;
	memcpy((char*)page + newSlotOffset - sizeof(unsigned), &freeSpaceOffset, sizeof(unsigned));
	//insert num of rids
	memcpy((char*)page + newSlotOffset - 2 * sizeof(unsigned), &numOfRids, sizeof(unsigned));
	//insert key
	//update info
	freeSpaceCapacity -= (RID_SIZE + 2 * sizeof(unsigned));
	freeSpaceOffset += RID_SIZE;
	switch (attribute.type)
	{
	case TypeInt:
		memcpy((char*)page + newSlotOffset - 2 * sizeof(unsigned) - sizeof(int), key, sizeof(int));
		freeSpaceCapacity -= sizeof(int);
		break;
	case TypeReal:
		memcpy((char*)page + newSlotOffset - 2 * sizeof(unsigned) - sizeof(float), key, sizeof(float));
		freeSpaceCapacity -= sizeof(float);
		break;
	case TypeVarChar:
		unsigned strLen= 0;
		memcpy(&strLen, key, sizeof(unsigned));
		memcpy((char*)page + newSlotOffset - 3 * sizeof(unsigned), &strLen, sizeof(unsigned));
		memcpy((char*)page + newSlotOffset - 3 * sizeof(unsigned) - strLen,
				(char*)key + sizeof(unsigned), strLen);
		freeSpaceCapacity -= (strLen + sizeof(unsigned));
		break;
	}
	//update info
	//update num of slots
	unsigned numOfKeySlots = 0;
	memcpy(&numOfKeySlots, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), sizeof(unsigned));
	numOfKeySlots++;
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), &numOfKeySlots, sizeof(unsigned));
	//update free space offset
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 3 * sizeof(unsigned), &freeSpaceOffset, sizeof(unsigned));
	//update free space capacity
	memcpy((char*)page + PAGE_SIZE - 2 * sizeof(int) - 2 * sizeof(unsigned), &freeSpaceCapacity, sizeof(unsigned));
	return;
}

unsigned IndexManager::getEntrySize(const Attribute &attribute, const void *key)
{
	unsigned totalSize = RID_SIZE + 2 * sizeof(unsigned);
	switch(attribute.type)
	{
	case TypeInt:
		totalSize += sizeof(int);
		break;
	case TypeReal:
		totalSize += sizeof(float);
		break;
	case TypeVarChar:
		unsigned strLen = 0;
		memcpy(&strLen, key, sizeof(unsigned));
		totalSize += (strLen + sizeof(unsigned));
		break;
	}
	return totalSize;
}

bool IndexManager::lookForKeySlotInPage(void *page, const Attribute &attribute, const void *key,
		unsigned &slotOffset, unsigned &keySlotNum)
{
	unsigned numOfKeySlots = 0;
	memcpy(&numOfKeySlots, (char*)page + PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned), sizeof(unsigned));
	unsigned i = 0;
	//next byte of the rightmost byte of the first slot
	unsigned keySlotOffset = PAGE_SIZE - 2 * sizeof(int) - 4 * sizeof(unsigned);
	while (i < numOfKeySlots)
	{
		unsigned keySlotLen = 0;
		int comp;
		switch (attribute.type)
		{
		case TypeInt:
			keySlotLen = sizeof(int) + 2 * sizeof(unsigned);
			comp = memcmp(key, (char*)page + keySlotOffset - keySlotLen, sizeof(int));
			if (!comp)//if find
			{
				slotOffset = keySlotOffset;//return next byte of the rightmost byte of slot
				keySlotNum = i;//return key slot num
				return true;
			}
			keySlotOffset -= keySlotLen;//go to the next byte of the rightmost part of next slot
			break;
		case TypeReal:
			keySlotLen = sizeof(float) + 2 * sizeof(unsigned);
			comp = memcmp(key, (char*)page + keySlotOffset - keySlotLen, sizeof(float));
			if (!comp)
			{
				slotOffset = keySlotOffset;//return next byte of the rightmost byte of slot
				keySlotNum = i;//return key slot num
				return true;
			}
			keySlotOffset -= keySlotLen;//go to the next byte of the rightmost part of next slot
			break;
		case TypeVarChar:
			unsigned strLen = 0;
			//read length of string
			memcpy(&strLen, (char*)page + keySlotOffset - 3 * sizeof(unsigned), sizeof(unsigned));
			keySlotLen = 3 * sizeof(unsigned) + strLen;
			comp = memcmp((char*)key + sizeof(unsigned), (char*)page + keySlotOffset - keySlotLen, strLen);
			if (!comp)
			{
				slotOffset = keySlotOffset;//return next byte of the rightmost byte of slot
				keySlotNum = i;//return key slot num
				return true;
			}
			keySlotOffset -= keySlotLen;//go to the next byte of the rightmost part of next slot
			break;
		}
		i++;
	}
	return false;
}

//find bucket to insert by given hashValue
unsigned IndexManager::getBucket(unsigned hashValue)
{
	unsigned totalPageOfCurrentLevel = baseNumOfPages * (1 << level);
	unsigned totalPageOfNextLevel = baseNumOfPages * (1 << (level + 1));
	unsigned bucketNum = hashValue % totalPageOfCurrentLevel;
	if (bucketNum < nextPointer) {
		bucketNum = hashValue % totalPageOfNextLevel;
	}
	return bucketNum;
}

RC IndexManager::getNumberOfPrimaryPages(IXFileHandle &ixfileHandle, unsigned &numberOfPrimaryPages) 
{
	numberOfPrimaryPages = baseNumOfPages * (1 << level) + nextPointer;
	return 0;
}

RC IndexManager::getNumberOfAllPages(IXFileHandle &ixfileHandle, unsigned &numberOfAllPages) 
{
	numberOfAllPages = baseNumOfPages * (1 << level) + nextPointer +
			numOfOverflowPages + numOfIndexPages + 1;
	return 0;
}

/**
 * chester
 */
void IndexManager::freeMemory(char* page_dummy) {
	if (page_dummy == NULL) {
		cout << "freeMemory" << endl;
		return;
	}
	free(page_dummy);
	page_dummy = NULL;
}

/**
 * read pageDummy from file
 */
void IndexManager::readPage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum, char* pageDummy) {
	// fileFlag == 0, read from primary page
	if(!fileFlag) {
		if (ixfileHandle.readPageFromPrimaryFile(pageNum, pageDummy)) {
			cout << "readPage : 1" << endl;
		}
	} else {
		if (ixfileHandle.readPageFromMetadataFile(pageNum, pageDummy)) {
			cout << "readPage : 2" << endl;
		}
	}
}

/**
 * write pageDummy into file
 */
void IndexManager::writePage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum, char* pageDummy) {
	// fileFlag == 0, read from primary page
	if(!fileFlag) {
		if (ixfileHandle.writePageToPrimaryFile(pageNum, pageDummy)) {
			cout << "writePage : 1" << endl;
		}
	} else {
		if (ixfileHandle.writePageToMetadataFile(pageNum, pageDummy)) {
			cout << "writePage : 2" << endl;
		}
	}
}


/**
 * delete entry given key and rid pair
 * 0 - success
 * -1 - not success
 * 1 - not success, didn't find matched pair
 */
RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid) {
	// get hashCode and find the bucket
	unsigned hashValue = hash(attribute, key);
	unsigned pageNum = getBucket(hashValue);
	// recursively find the <key, rid> pair
	unsigned fileFlag = 0, preFlag = 0; // 0 - primary, 1 - overflow
	int prePage = -1, nextPage = -1;
	unsigned freespaceOffset = 0;
	unsigned pageNumCopy = pageNum;
	RC res = -1;
	while(true) {
		res = deleteEntryFromPage(ixfileHandle, fileFlag, pageNumCopy, attribute, key, rid, preFlag, prePage, nextPage, freespaceOffset);
		if(!res || nextPage == -1) {
			break;
		}
		fileFlag = 1;
		pageNumCopy = nextPage;
	}
	// if didn't find matched pair
	if(res == -1) {
		IX_PrintError(1);
		return 1;
	}
	// check whether current page is empty
	if(fileFlag && freespaceOffset == 0) {
		deleteEmptyPage(ixfileHandle, fileFlag, pageNumCopy, preFlag, prePage, nextPage);
		// check whether need to merge
		if(needMerge(ixfileHandle)) {
			// get the buckets needed to be merged
			mergeBucket(ixfileHandle);
		}
	}

	return 0;
}

/**
 * delete the key-rid pair given page number and key-rid pair
 * 0 - success
 * -1 - not success, didn't find matched pair
 */
RC IndexManager::deleteEntryFromPage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum,
		const Attribute &attribute, const void *key, const RID &rid, unsigned &preFlag, int &prePage, int &nextPage, unsigned &freespaceOffset) {
	char* pageDummy = (char*) malloc(PAGE_SIZE);
	assert(pageDummy != NULL);
	// read the pageDummy
	readPage(ixfileHandle, fileFlag, pageNum, pageDummy);
	// resolve the key
	int valueInt = 0;
	float valueReal = 0;
	unsigned charLength = 0;
	char* valueVChar = NULL;
	string valueString;
	// resolve the key
	resolveKey(attribute, key, valueInt, valueReal, charLength, valueVChar, valueString);
	// get the page info
	unsigned freespaceLength;
	unsigned numOfKeySlot;
	getRidPageInfo(pageDummy, nextPage, prePage, preFlag, freespaceLength, freespaceOffset, numOfKeySlot);
	// initialize
	unsigned keySlotPointer = 0;
	unsigned ridOffset = 0;
	unsigned numOfRid = 0;
	// find the matched key slot
	findKeySlot(pageDummy, attribute, valueInt, valueReal, valueString, numOfKeySlot, keySlotPointer, ridOffset, numOfRid);
	// check whether find the specific pair
	// check whether the rid list is empty
	if(keySlotPointer == numOfKeySlot || numOfRid == 0) {
		if(valueVChar != NULL) {
			freeMemory(valueVChar);
		}
		freeMemory(pageDummy);
		return -1;
	}
	// find the rid needed to be deleted
	RC res = -1;
	RID tempRid = {0, 0};
	unsigned offset = ridOffset;
	int ridPointer = 0;
	for(; ridPointer < numOfRid; ridPointer++) {
		memcpy(&tempRid.pageNum, pageDummy+offset, sizeof(unsigned));
		memcpy(&tempRid.slotNum, pageDummy+offset+sizeof(unsigned), sizeof(unsigned));
		if(tempRid.pageNum == rid.pageNum && tempRid.slotNum == rid.slotNum) {
			coverDeleted(pageDummy, attribute, keySlotPointer, ridPointer);
			freespaceOffset -= RID_SIZE;
			res = 0;
			break;
		}
		offset += RID_SIZE;
	}
	// update file
	writePage(ixfileHandle, fileFlag, pageNum, pageDummy);
	// free memory
	if(valueVChar != NULL) {
		freeMemory(valueVChar);
	}
	freeMemory(pageDummy);
	return res;
}

/**
 * delete the page
 * update its pre and next node
 */
void IndexManager::deleteEmptyPage(IXFileHandle &ixfileHandle, unsigned fileFlag, unsigned pageNum, unsigned preFlag, int prePage, int nextPage) {
	char* pageDummy = (char*) malloc(PAGE_SIZE);
	assert(pageDummy != NULL);
	// if overflow page is empty
	if(fileFlag != 0) {
		// update the nextPage pointer in pre page
		readPage(ixfileHandle, preFlag, prePage, pageDummy);
		unsigned offset = PAGE_SIZE - sizeof(int);
		memcpy(pageDummy + offset, &nextPage, sizeof(int));
		writePage(ixfileHandle, preFlag, prePage, pageDummy);
		// update the prePage pointer in next page
		if (nextPage != -1) {
			readPage(ixfileHandle, 1, nextPage, pageDummy);
			offset = PAGE_SIZE - sizeof(int) * 2;
			memcpy(pageDummy + offset, &prePage, sizeof(int));
			offset -= sizeof(int);
			memcpy(pageDummy + offset, &preFlag, sizeof(unsigned));
			writePage(ixfileHandle, 1, nextPage, pageDummy);
		}
		// add the empty page into free page directory
		addFreePage(ixfileHandle, pageNum, 1);
	}
	/**
	// if primary page is empty, no need to delete
	if (fileFlag == 0) {
		if (nextPage != -1) {
			readPage(ixfileHandle, 1, nextPage, pageDummy);
			// get the next next page
			unsigned offset = PAGE_SIZE - sizeof(int);
			int nextNextPage = 0;
			memcpy(&nextNextPage, pageDummy + offset, sizeof(int));
			// update the prePage pointer and move it to primary page
			offset = PAGE_SIZE - sizeof(int);
			int prePageValue = -1;
			memcpy(pageDummy + offset, &prePageValue, sizeof(int));
			unsigned primaryPageFlag = 0;
			writePage(ixfileHandle, primaryPageFlag, pageNum, pageDummy);
			// update the prePage pointer of the next next page
			if (nextNextPage != -1) {
				readPage(ixfileHandle, 1, nextNextPage, pageDummy);
				// update the prePage
				offset = PAGE_SIZE - sizeof(int) * 2;
				memcpy(pageDummy + offset, &pageNum, sizeof(int));
				// update the preFlag
				offset = PAGE_SIZE - sizeof(int);
				unsigned preFlagValue = 0;
				memcpy(pageDummy + offset, &preFlagValue, sizeof(unsigned));
				writePage(ixfileHandle, 1, nextNextPage, pageDummy);
			}
			addIntoFreePageDir(nextPage);
		}
	}
	 */
	freeMemory(pageDummy);
}

/**
 * check whether need to merge
 */
bool IndexManager::needMerge(IXFileHandle &ixfileHandle) {
	unsigned numOfBucket = 0;
	getNumberOfPrimaryPages(ixfileHandle, numOfBucket);
	if(numOfBucket <= baseNumOfPages) {
		return false;
	} else {
		return true;
	}
}

/**
 * merge bucket
 */
void IndexManager::mergeBucket(IXFileHandle &ixfileHandle) {
	// get the buckets needed to be merged
	unsigned originBucket = 0;
	unsigned splitBucket = 0;
	if (nextPointer == 0) {
		splitBucket = (unsigned) (pow(2, level)) * baseNumOfPages - 1;
		originBucket = splitBucket - (unsigned) (pow(2, level - 1)) * baseNumOfPages;
	} else {
		originBucket = nextPointer - 1;
		splitBucket = originBucket + (unsigned) (pow(2, level)) * baseNumOfPages;
	}
	// get the tail of the origin bucket
	unsigned originFileFlag = 0; // start from primary page
	unsigned originBucketTail = originBucket;
	char* originPageDummy = (char*) malloc(PAGE_SIZE);
	assert(originPageDummy != NULL);
	unsigned pageOffset = PAGE_SIZE - sizeof(int);
	while (true) {
		readPage(ixfileHandle, originFileFlag, originBucketTail, originPageDummy);
		// get the next page
		int originNextPage = 0;
		memcpy(&originNextPage, originPageDummy + pageOffset, sizeof(int));
		if (originNextPage == -1) {
			break;
		}
		originFileFlag = 1; // overflow page
		originBucketTail = originNextPage;
	}
	// get the head of split bucket
	unsigned splitFileFlag = 0;
	unsigned splitBucketTail = splitBucket;
	char* splitPageDummy = (char*) malloc(PAGE_SIZE);
	assert(splitPageDummy != NULL);
	readPage(ixfileHandle, splitFileFlag, splitBucketTail, splitPageDummy);
	// update split page
	unsigned overflowPageFlag = 1;
	pageOffset = PAGE_SIZE - sizeof(int) * 2;
	memcpy(splitPageDummy + pageOffset, &originBucketTail, sizeof(int));
	pageOffset -= sizeof(int);
	memcpy(splitPageDummy + pageOffset, &originFileFlag, sizeof(int));
	unsigned newOverflowPageNum = 0;
	getFreePage(ixfileHandle, newOverflowPageNum, overflowPageFlag);
	writePage(ixfileHandle, overflowPageFlag, newOverflowPageNum, splitPageDummy);
	// update page after split page
	pageOffset = PAGE_SIZE - sizeof(int);
	int nextSplitPage = 0;
	memcpy(&nextSplitPage, splitPageDummy + pageOffset, sizeof(int));
	if (nextSplitPage != -1) {
		readPage(ixfileHandle, overflowPageFlag, nextSplitPage, splitPageDummy);
		// update the prePage
		pageOffset -= sizeof(int);
		memcpy(splitPageDummy + pageOffset, &newOverflowPageNum, sizeof(int));
		// update the preFlag
		pageOffset -= sizeof(int);
		memcpy(splitPageDummy + pageOffset, &overflowPageFlag, sizeof(unsigned));
		writePage(ixfileHandle, overflowPageFlag, nextSplitPage, splitPageDummy);
	}
	// update tail of origin page
	pageOffset = PAGE_SIZE - sizeof(int);
	// update nextPage
	memcpy(originPageDummy + pageOffset, &newOverflowPageNum, sizeof(unsigned));
	writePage(ixfileHandle, originFileFlag, originBucketTail, originPageDummy);
	freeMemory(originPageDummy);
	freeMemory(splitPageDummy);
}


/**
 * get pageInfo (the last six data)
 */
void IndexManager::getRidPageInfo(const char* pageDummy, int &nextPage, int &prePage, unsigned &preFlag,
		unsigned &freespaceLength, unsigned &freespaceOffset, unsigned &numOfKeySlot) {
	// get next page
	unsigned pageOffset = PAGE_SIZE - sizeof(int);
	memcpy(&nextPage, pageDummy+pageOffset, sizeof(int));
	// get pre page
	pageOffset -= sizeof(int);
	memcpy(&prePage, pageDummy+pageOffset, sizeof(int));
	// get pre flag
	pageOffset -= sizeof(unsigned);
	memcpy(&preFlag, pageDummy+pageOffset, sizeof(unsigned));
	// get free space length
	pageOffset -= sizeof(unsigned);
	memcpy(&freespaceLength, pageDummy+pageOffset, sizeof(unsigned));
	// get free space offset
	pageOffset -= sizeof(unsigned);
	memcpy(&freespaceOffset, pageDummy+pageOffset, sizeof(unsigned));
	// get the key slot number
	pageOffset -= sizeof(unsigned);
	memcpy(&numOfKeySlot, pageDummy+pageOffset, sizeof(unsigned));
}

/**
 * resolve the key information
 */
void IndexManager::resolveKey(const Attribute &attribute, const void *key, int &valueInt, float &valueReal,
		unsigned &charLength, char* &valueVChar, string &valueString) {
	char tail = '\0';
	if(attribute.type == TypeInt) {
		memcpy(&valueInt, key, sizeof(int));
	} else if(attribute.type == TypeReal) {
		memcpy(&valueReal, key, sizeof(float));
	} else if(attribute.type == TypeVarChar) {
		memcpy(&charLength, key, sizeof(unsigned));
		valueVChar = (char *) (malloc(charLength + 1));
		assert(valueVChar);
		memcpy(valueVChar, (char*) (key) + sizeof(unsigned), charLength);
		memcpy(valueVChar + charLength, &tail, sizeof(char));
		valueString = string(valueVChar);
	} else {
		cout << "resolveKey" << endl;
	}
}

/**
 * given the key information, find the key slot in a page
 */
void IndexManager::findKeySlot(char* pageDummy, const Attribute &attribute, int valueInt, float valueReal, string valueString,
		unsigned numOfKeySlot, unsigned &keySlotPointer, unsigned &ridOffset, unsigned &numOfRid) {
	char tail = '\0';
	unsigned pageOffset = PAGE_SIZE - sizeof(int) * 6;
	// keySlotPointer initialized to 0
	for (; keySlotPointer < numOfKeySlot; keySlotPointer++) {
		// get the rid offset
		pageOffset -= sizeof(unsigned);
		memcpy(&ridOffset, pageDummy + pageOffset, sizeof(int));
		// get the number of rid
		pageOffset -= sizeof(unsigned);
		memcpy(&numOfRid, pageDummy + pageOffset, sizeof(int));
		// get the value
		pageOffset -= sizeof(int);
		if (attribute.type == TypeInt) {
			int value = 0;
			memcpy(&value, pageDummy + pageOffset, sizeof(int));
			if (value == valueInt) {
				break;
			}
		} else if (attribute.type == TypeReal) {
			float value = 0;
			memcpy(&value, pageDummy + pageOffset, sizeof(float));
			if (value == valueReal) {
				break;
			}
		} else if (attribute.type == TypeVarChar) {
			unsigned length = 0;
			memcpy(&length, pageDummy + pageOffset, sizeof(unsigned));
			pageOffset -= length;
			// get the value from slot
			char* data = (char *) malloc(length + 1);
			assert(data != NULL);
			memcpy(data, pageDummy + pageOffset, length);
			memcpy(data + length, &tail, sizeof(char));
			string value(data);
			freeMemory(data);
			if (!value.compare(valueString)) {
				break;
			}
		} else {
			cout << "findKeyRid" << endl;
		}
	}
}

/**
 * delete the given rid
 * achieved by cover current rid with backward rids
 */
void IndexManager::coverDeleted(char* pageDummy, const Attribute &attribute, int keySlotPointer, int ridPointer) {
	int nextPage, prePage;
	unsigned preFlag, freespaceLength, freespaceOffset, numOfKeySlot;
	getRidPageInfo(pageDummy, nextPage, prePage, preFlag, freespaceLength, freespaceOffset, numOfKeySlot);
	// delete within own rid list
	unsigned pageOffset = PAGE_SIZE - sizeof(int) * 6;
	unsigned preOffset = pageOffset;
	unsigned curOffset = pageOffset;
	for(int i = 0; i < keySlotPointer; i++) {
		getCurKeySlotOffset(pageDummy, attribute, preOffset, curOffset);
		preOffset = curOffset;
	}
	// get ridOffset
	curOffset -= sizeof(unsigned);
	unsigned ridOffset = 0;
	memcpy(&ridOffset, pageDummy + curOffset, sizeof(unsigned));
	// get number of rid
	curOffset -= sizeof(unsigned);
	unsigned numOfRid = 0;
	memcpy(&numOfRid, pageDummy + curOffset, sizeof(unsigned));
	if(ridPointer + 1 != numOfRid) {
		memcpy(pageDummy + ridOffset + ridPointer * RID_SIZE, pageDummy + ridOffset + (numOfRid - 1) * RID_SIZE, RID_SIZE);
	}
	// update the number of rid after deleting
	unsigned newNumOfRid = numOfRid - 1;
	memcpy(pageDummy + curOffset, &newNumOfRid, sizeof(unsigned));
	// iteratively cover
	unsigned preDeletedRidOffset = ridOffset + (numOfRid - 1) * RID_SIZE;
	unsigned curDeletedRidOffset = 0;
	unsigned keySlotPointerCopy = keySlotPointer;
	while(keySlotPointerCopy < numOfKeySlot - 1) {
		getCurKeySlotOffset(pageDummy, attribute, preOffset, curOffset);
		preOffset = curOffset;
		// get current rid offset
		curOffset -= sizeof(unsigned);
		unsigned curRidOffset = 0;
		memcpy(&curRidOffset, pageDummy + curOffset, sizeof(unsigned));
		// get current number of rid
		curOffset -= sizeof(unsigned);
		unsigned curNumOfRid = 0;
		memcpy(&curNumOfRid, pageDummy + curOffset, sizeof(unsigned));
		if(curNumOfRid != 0) {
			curDeletedRidOffset = curRidOffset + (curNumOfRid - 1) * RID_SIZE;
			// cover
			memcpy(pageDummy + preDeletedRidOffset, pageDummy + curDeletedRidOffset, RID_SIZE);
			// update
			preDeletedRidOffset = curDeletedRidOffset;
		}
		keySlotPointerCopy++;
	}
	// update the directory
	pageOffset = PAGE_SIZE - sizeof(int) * 3;
	// update free space length
	pageOffset -= sizeof(unsigned);
	freespaceLength += RID_SIZE;
	memcpy(pageDummy + pageOffset, &freespaceLength, sizeof(unsigned));
	// update free space offset
	pageOffset -= sizeof(unsigned);
	freespaceOffset -= RID_SIZE;
	memcpy(pageDummy + pageOffset, &freespaceOffset, sizeof(unsigned));
	// update slot offset, rid number of key slots after keySlotPointer
	pageOffset = PAGE_SIZE - sizeof(int) * 6;
	preOffset = pageOffset;
	curOffset = pageOffset;
	// might have some problems
	for(int i = 0; i < keySlotPointer; i++) {
		getCurKeySlotOffset(pageDummy, attribute, preOffset, curOffset);
		preOffset = curOffset;
	}
	keySlotPointerCopy = keySlotPointer + 1;
	while(keySlotPointerCopy < numOfKeySlot) {
		getCurKeySlotOffset(pageDummy, attribute, preOffset, curOffset);
		preOffset = curOffset;
		// get current rid offset
		curOffset -= sizeof(unsigned);
		unsigned curRidOffset = 0;
		memcpy(&curRidOffset, pageDummy + curOffset, sizeof(unsigned));
		// update current rid offset
		curRidOffset -= RID_SIZE;
		memcpy(pageDummy + curOffset, &curRidOffset, sizeof(unsigned));
		// update
		preDeletedRidOffset = curDeletedRidOffset;
		keySlotPointerCopy++;
	}
}

/**
 * get current offset of the key slot
 * by given the previous offset of the key slot
 */
void IndexManager::getCurKeySlotOffset(char* pageDummy, const Attribute &attribute, unsigned preOffset, unsigned &curOffset) {
	if(pageDummy == NULL) {
		cout << "getCurKeySlotOffset: pageDummy == NULL" << endl;
		return;
	}
	// get length of the key
	unsigned offset = preOffset - sizeof(unsigned) * 3;
	unsigned length = 0;
	switch(attribute.type) {
	case TypeInt:
		break;
	case TypeReal:
		break;
	case TypeVarChar:
		memcpy(&length, pageDummy + offset, sizeof(unsigned));
		break;
	default:
		break;
	}
	// set the current offset
	curOffset = offset - length;
}

unsigned IndexManager::hash(const Attribute &attribute, const void *key) {
	unsigned hash = 0;
	switch(attribute.type) {
	case TypeInt: {
		int intKey = 0;
		memcpy(&intKey, key, sizeof(int));
		hash = hashInt(intKey);
		break;
	}
	case TypeReal: {
		float floatKey = 0.0;
		memcpy(&floatKey, key, sizeof(int));
		hash = hashReal(floatKey);
		break;
	}
	case TypeVarChar: {
		int length = 0;
		memcpy(&length, key, sizeof(unsigned));
		char* charKey = (char*) malloc(length + 1);
		assert(charKey != NULL);
		memcpy(charKey, (char*) key + sizeof(unsigned), length);
		char tail = '\0';
		memcpy(charKey + length, &tail, sizeof(char));
		hash = hashVarChar(charKey);
		freeMemory(charKey);
		break;
	}
	}
	return hash;
}

unsigned IndexManager::hashInt(int key) {
	// derived from Robert Jenkin's idea
	// http://cato.zxq.net/?p=110
	key = ~key + (key << 15); // key = (key << 15) - key - 1;
	key = key ^ (key >> 12);
	key = key + (key << 2);
	key = key ^ (key >> 4);
	key = key * 2057; // key = (key + (key << 3)) + (key << 11);
	key = key ^ (key >> 16);
	// map int to unsigned
	return (unsigned) key;

}

unsigned IndexManager::hashReal(float key) {
//    unsigned hash;
//    memcpy(&hash, &key, sizeof(float));
	unsigned hash = (unsigned) key;
	// unsigned mask = 0xfffffffe;
	return hash;
}

unsigned IndexManager::hashVarChar(char* key) {
	// https://www.byvoid.com/blog/string-hash-compare
	// unsigned seed = 131; // 31 131 1313 13131 etc..
	// unsigned hash = 0;
	// while(*key != '\0') {
	//	hash = hash * seed + (*key++);
	// }
	// return (hash & 0x7fffffff);
    //
    
    // http://stackoverflow.com/questions/8317508/hash-function-for-a-string
    unsigned A = 54059; /* a prime */
    unsigned B = 76963; /* another prime */
    unsigned C = 86969; /* yet another prime */

    unsigned h = 31 /* also prime */;
    while (*key != '\0') {
        h = (h * A) ^ (key[0] * B);
        key++;
    }
    return h % C; // or return h % C;

}

RC IndexManager::printIndexEntriesInAPage(IXFileHandle &ixfileHandle, const Attribute &attribute, const unsigned &primaryPageNumber) {
	unsigned pageFlag = 0, prePageFlag = 0;
	int pagePointer = (int) primaryPageNumber;
	int prePagePointer = pagePointer;
	char* pageDummy = (char*) malloc(PAGE_SIZE);
	assert(pageDummy != NULL);
	queue<KeySlot*> resultList;
	unsigned totalEntryCount = 0;
	unsigned pageEntryCount = 0;
	while(true) {
		if(pagePointer == -1) {
			break;
		}
		// initialize parameters
		pageEntryCount = 0;
		readPage(ixfileHandle, pageFlag, pagePointer, pageDummy);
		// get the list of KeySlot
		unsigned pageOffset = PAGE_SIZE - sizeof(int) * 6;
		unsigned numOfSlots = 0;
		memcpy(&numOfSlots, pageDummy + pageOffset, sizeof(unsigned));
		unsigned curOffset = 0;
		// clear the current keySlotList
		queue<KeySlot*> empty;
		swap(resultList, empty);
		for(int i = 0; i < numOfSlots; i++) {
			KeySlot *keySlot = new KeySlot;
			(*keySlot).key = NULL;
			(*keySlot).keyLength = sizeof(int);
			(*keySlot).numOfRids = 0;
			(*keySlot).ridOffset = 0;
			getCurKeySlot(pageDummy, attribute, pageOffset, curOffset, keySlot->ridOffset, keySlot->numOfRids, keySlot->key);
			pageOffset = curOffset;
			if(keySlot->numOfRids != 0) {
				resultList.push(keySlot);
				pageEntryCount += keySlot->numOfRids;
			} else {
				delete keySlot;
				keySlot = NULL;
			}
		}
		// print result
		// print
		if(!pageFlag) {
			cout << "primary Page NO. " << pagePointer << endl;
		} else {
			cout << "overflow Page NO. " << pagePointer << " linked to ";
			if(!prePageFlag) {
				cout << "primary page " << prePagePointer << endl;
			} else {
				cout << "overflow page " << prePagePointer << endl;
			}
		}
		cout << "# of entries: " << pageEntryCount << endl;
		cout << "entries:\t";

		while(!resultList.empty()) {
			KeySlot* keySlot = (KeySlot*) resultList.front();
			resultList.pop();
			int intKey = 0;
			float floatKey = 0.0;
			unsigned keyLength = 0;
			string stringKey;
			switch(attribute.type) {
			case TypeInt: {
				memcpy(&intKey, keySlot->key, sizeof(int));
				break;
			}
			case TypeReal: {
				memcpy(&floatKey, keySlot->key, sizeof(float));
				break;
			}
			case TypeVarChar: {
				memcpy(&keyLength, keySlot->key, sizeof(unsigned));
				char* charKey = (char *) malloc(keyLength + 1);
				memcpy(charKey, keySlot->key, keyLength);
				char tail = '\0';
				memcpy(charKey + keyLength, &tail, sizeof(char));
				stringKey = string(charKey);
				freeMemory(charKey);
				break;
			}
			default: break;
			}
			unsigned ridOffset = keySlot->ridOffset;
			for(int i = 0; i < keySlot->numOfRids; i++) {
				cout << "[ ";
				unsigned pageNumber = 0;
				unsigned slotNumber = 0;
				memcpy(&pageNumber, pageDummy + ridOffset, sizeof(unsigned));
				memcpy(&slotNumber, pageDummy + ridOffset + sizeof(unsigned), sizeof(unsigned));
				switch(attribute.type) {
				case TypeInt: {
					cout << intKey;
					break;
				}
				case TypeReal: {
					cout << floatKey;
					break;
				}
				case TypeVarChar: {
					cout << stringKey;
					break;
				}
				default : break;
				}
				cout << " / " << pageNumber << ", " << slotNumber << " ]\t";
				ridOffset += RID_SIZE;
			}
			cout << endl;
			// freeMemory
			freeMemory((char *) keySlot->key);
			if(keySlot != NULL) {
				delete keySlot;
				keySlot = NULL;
			}
		}
		// update the pageFlag and pagePointer
		prePageFlag = pageFlag;
		pageFlag = 1;
		prePagePointer = pagePointer;
		pageOffset = PAGE_SIZE - sizeof(int);
		memcpy(&pagePointer, pageDummy + pageOffset, sizeof(int));
		totalEntryCount += pageEntryCount;
	}
	cout << "Number of total entries in the page (+ overflow pages): " << totalEntryCount << endl;
	return 0;
}

/**
 * given preOffset
 * get the current key slot info
 * curOffset is the offset of current key slot
 */
void IndexManager::getCurKeySlot(char* pageDummy, const Attribute &attribute, unsigned preOffset, unsigned &curOffset, unsigned &ridOffset,
		unsigned &numOfRids, void* &key) {

	// get rid offset
	unsigned offset = preOffset - sizeof(unsigned);
	memcpy(&ridOffset, pageDummy + offset, sizeof(unsigned));
	// get the num of rids
	offset -= sizeof(unsigned);
	memcpy(&numOfRids, pageDummy + offset, sizeof(unsigned));
	unsigned length = sizeof(unsigned);
	switch(attribute.type) {
	case TypeInt:
		break;
	case TypeReal:
		break;
	case TypeVarChar: {
		unsigned tempOffset = offset - sizeof(unsigned);
		memcpy(&length, pageDummy + tempOffset, sizeof(unsigned));
		break;
	}
	default:
		break;
	}
	// get the key
	if(key == NULL) {
        if(attribute.type == TypeVarChar) {
            key = malloc(length + sizeof(unsigned));
            assert(key != NULL);
        } else {
            key = malloc(sizeof(unsigned));
            assert(key != NULL);
        }
	} else {
		cout << "IndexManager: getCurKeySlot" << endl;
	}
	offset -= sizeof(unsigned);
	if(attribute.type != TypeVarChar) {
		memcpy(key, pageDummy + offset, sizeof(unsigned));
	} else {
		memcpy(key, pageDummy + offset, sizeof(unsigned));
		offset -= length;
		memcpy((char*) key + sizeof(unsigned), pageDummy + offset, length);
	}
	// set the current offset
	curOffset = offset;
}

RC IndexManager::scan(IXFileHandle &ixfileHandle, const Attribute &attribute,
		const void *lowKey, const void *highKey, bool lowKeyInclusive,
		bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator) {
    ix_ScanIterator.resetOthers();
	ix_ScanIterator.setIXFileHandle(ixfileHandle);
	ix_ScanIterator.setAttribute(attribute);
	ix_ScanIterator.setLowKey(lowKey);
	ix_ScanIterator.setHighKey(highKey);
	ix_ScanIterator.setLowKeyInclusive(lowKeyInclusive);
	ix_ScanIterator.setHighKeyInclusive(highKeyInclusive);
	// get the primary page num
	unsigned numOfBuckets = 0;
	getNumberOfPrimaryPages(ixfileHandle, numOfBuckets);
	ix_ScanIterator.setPrimaryPageCache(numOfBuckets);
	// get the overflow page num
	queue<unsigned> overflowPageCache;
	getInUseOverflowPages(ixfileHandle, overflowPageCache);
	ix_ScanIterator.setOverflowPageCache(overflowPageCache);
	return 0;
}

/**
 * get a queue of non-empty overflow pages
 */
void IndexManager::getInUseOverflowPages(IXFileHandle &ixfileHandle, queue<unsigned> &overflowPageCache) {
	int numOfPages = ixfileHandle.getNumOfPagesInMetadataFile();
	bool* mask = (bool*) malloc(sizeof(bool) * numOfPages);
	assert(mask != NULL);
	memset(mask, true, sizeof(bool) * numOfPages);
	mask[0] = false; // first page is metadata page
	unsigned overflowPageFlag = 1;
	int pagePointer = 1;
	char* pageDummy = (char*) malloc(PAGE_SIZE);
	assert(pageDummy != NULL);
	while(true) {
		if(pagePointer == -1) {
			break;
		}
		mask[pagePointer] = false;
		readPage(ixfileHandle, overflowPageFlag, pagePointer, pageDummy);
		// update the next pagePointer
		unsigned offset = PAGE_SIZE - sizeof(int);
		memcpy(&pagePointer, pageDummy + offset, sizeof(int));
		// get the number of empty overflow page
		offset -= sizeof(int) * 2;
		unsigned numOfOverflowPages = 0;
		memcpy(&numOfOverflowPages, pageDummy + offset, sizeof(unsigned));
		unsigned pageNum = 0;
		offset = 0;
		for(int i = 0; i < numOfOverflowPages; i++) {
			memcpy(&pageNum, pageDummy + offset, sizeof(unsigned));
			mask[pageNum] = false;
			offset += sizeof(unsigned);
		}
	}
	// add non-empty overflow pages into queue
	for(int i = 0; i < numOfPages; i++) {
		if(mask[i]) {
			overflowPageCache.push(i);
		}
	}
	// free memory
	freeMemory((char*) mask);
	freeMemory(pageDummy);
}

// ============================================================================================================

IX_ScanIterator::IX_ScanIterator():lowKey(NULL), highKey(NULL) {
	//	curKeySlot = new KeySlot;
	//	(*curKeySlot).key = NULL;
	//	(*curKeySlot).keyLength = sizeof(unsigned);
	//	(*curKeySlot).numOfRids = 0;
	//	(*curKeySlot).ridOffset = 0;
	ridPointer = 0;
	pageDummy = (char*) malloc(PAGE_SIZE);
	preRid = new RID;
	(*preRid).pageNum = 0xffffffff;
	(*preRid).slotNum = 0xffffffff;
	isStart = true;
}

IX_ScanIterator::~IX_ScanIterator() {
	if(pageDummy!= NULL) {
		free(pageDummy);
		pageDummy = NULL;
	}
//	if(curKeySlot != NULL) {
//		delete curKeySlot;
//		curKeySlot = NULL;
//	}
	if(preRid != NULL) {
		delete preRid;
		preRid = NULL;
	}
}


/**
 * get the next entry
 * 0 - success
 * IX_EOF(-1) - end of the file
 */
RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {

	// get new page and initialize parameters
	if(isStart) {
		if(iterNewPage()) {
			cout << "getNextEntry: iterNewPage" << endl;
			return -1;
		}
		isStart = false;
	}
	if(findCurEntry(rid, key)) {
		cout << "getNextEngry: findCurEntry" << endl;
		return -1;
	}
	if(rid.pageNum == preRid->pageNum && rid.slotNum == preRid->slotNum) {
		if(findNextEntry(rid, key)) {
			return -1;
		}
	}
	preRid->pageNum = rid.pageNum;
	preRid->slotNum = rid.slotNum;
	return 0;
}

/**
 * read a new page from queue
 * resolve key slot info
 * 0 - success
 * -1 - fail, no new pages
 */
RC IX_ScanIterator::iterNewPage() {
	RC res = -1;
	if(!primaryPageCache.empty()) {
		unsigned pagePointer = (unsigned) primaryPageCache.front();
		primaryPageCache.pop();
		ixfileHandle.readPageFromPrimaryFile(pagePointer, pageDummy);
		res = 0;
	}
	if(res && !overflowPageCache.empty()) {
		unsigned pagePointer = (unsigned) overflowPageCache.front();
		overflowPageCache.pop();
		ixfileHandle.readPageFromMetadataFile(pagePointer, pageDummy);
		res = 0;
	}
	if(res) return res;
	// get the list of KeySlot
	unsigned pageOffset = PAGE_SIZE - sizeof(int) * 6;
	unsigned numOfSlots = 0;
	memcpy(&numOfSlots, pageDummy + pageOffset, sizeof(unsigned));
	unsigned curOffset = 0;
	// clear the current keySlotList
	queue<KeySlot*> empty;
	swap(keySlotList, empty);
	for(int i = 0; i < numOfSlots; i++) {
		KeySlot *keySlot = new KeySlot;
		(*keySlot).key = NULL;
		(*keySlot).keyLength = sizeof(unsigned);
		(*keySlot).numOfRids = 0;
		(*keySlot).ridOffset = 0;
		getCurKeySlot(pageOffset, curOffset, keySlot->ridOffset, keySlot->numOfRids, keySlot->key, keySlot->keyLength);
		pageOffset = curOffset;
		if(isKeyValid(keySlot->key) && keySlot->numOfRids != 0) {
			keySlotList.push(keySlot);
		} else {
			if(keySlot != NULL) {
				delete keySlot;
				keySlot = NULL;
			}
		}
	}
	// if queue is empty, iter new page again
	if(keySlotList.size() == 0) {
		return iterNewPage();
	}
	// reset the parameters
	curKeySlot = (KeySlot*) keySlotList.front();
	keySlotList.pop();
	ridPointer = 0;
	return 0;
}

/**
 * given preOffset
 * get the current key slot info
 * curOffset is the offset of current key slot
 */
void IX_ScanIterator::getCurKeySlot(unsigned preOffset, unsigned &curOffset, unsigned &ridOffset,
		unsigned &numOfRids, void* &key, unsigned &keyLength) {
	if(pageDummy == NULL) {
		cout << "getCurKeySlot: pageDummy == NULL" << endl;
		return;
	}
	// get rid offset
	unsigned offset = preOffset - sizeof(unsigned);
	memcpy(&ridOffset, pageDummy + offset, sizeof(unsigned));
	// get the num of rids
	offset -= sizeof(unsigned);
	memcpy(&numOfRids, pageDummy + offset, sizeof(unsigned));
	unsigned length = sizeof(unsigned);
	switch(attribute.type) {
	case TypeInt:
		break;
	case TypeReal:
		break;
	case TypeVarChar: {
		unsigned tempOffset = offset - sizeof(unsigned);
		memcpy(&length, pageDummy + tempOffset, sizeof(unsigned));
		break;
	}
	default:
		break;
	}
	// get the key
	if(key == NULL) {
        if(attribute.type == TypeVarChar) {
            key = malloc(length + sizeof(unsigned));
            assert(key != NULL);
        } else  {
            key = malloc(sizeof(unsigned));
            assert(key != NULL);
        }
	} else {
		cout << "getCurKeySlot" << endl;
	}
	offset -= sizeof(unsigned);
	if(attribute.type != TypeVarChar) {
		memcpy(key, pageDummy + offset, sizeof(unsigned));
		keyLength = sizeof(unsigned);
	} else {
		memcpy(key, pageDummy + offset, sizeof(unsigned));
		offset -= length;
		memcpy((char*) key + sizeof(unsigned), pageDummy + offset, length);
		keyLength = length + sizeof(unsigned);
	}
	// set the current offset
	curOffset = offset;
}

/**
 * check whether the key is valid
 */
bool IX_ScanIterator::isKeyValid(void* key) {
	bool leftRes = false, rightRes = false;
	switch(attribute.type) {
	case TypeInt: {
		int intLowKeyValue = 0, intHighKeyValue = 0;
		if(lowKey != NULL) {
			memcpy(&intLowKeyValue, lowKey, sizeof(int));
		} else {
			leftRes = true;
		}
		if(highKey != NULL) {
			memcpy(&intHighKeyValue, highKey, sizeof(int));
		} else {
			rightRes = true;
		}
		int intKeyValue = 0;
		memcpy(&intKeyValue, key, sizeof(int));


		if(!leftRes && lowKeyInclusive && intLowKeyValue == intKeyValue) {
			return true;
		}
		if(!rightRes && highKeyInclusive && intHighKeyValue == intKeyValue) {
			return true;
		}
		if((leftRes || intLowKeyValue < intKeyValue) && (rightRes || intKeyValue < intHighKeyValue)) {
			return true;
		}
		break;
	}
	case TypeReal: {
		float floatLowKeyValue = 0.0, floatHighKeyValue = 0.0;
		if(lowKey != NULL) {
			memcpy(&floatLowKeyValue, lowKey, sizeof(float));
		} else {
			leftRes = true;
		}
		if(highKey != NULL) {
			memcpy(&floatHighKeyValue, highKey, sizeof(float));
		} else {
			rightRes = true;
		}
		float floatKeyValue = 0.0;
		memcpy(&floatKeyValue, key, sizeof(float));
		if(!leftRes && lowKeyInclusive && fabs(floatLowKeyValue - floatKeyValue) < EPSILON) {
			return true;
		}
		if(!rightRes && highKeyInclusive && fabs(floatHighKeyValue - floatKeyValue) < EPSILON) {
			return true;
		}
		if((leftRes || floatLowKeyValue < floatKeyValue) && (rightRes || floatKeyValue < floatHighKeyValue)) {
			return true;
		}
		break;
	}
	case TypeVarChar: {
		// unsigned size = sizeof(key);
		unsigned length = 0;
		memcpy(&length, key, sizeof(unsigned));
		// get the result of comparison
		int lowKeyCompare = 0;
		if(lowKey != NULL) {
			lowKeyCompare = memcmp((char *) lowKey + sizeof(unsigned), (char *) key + sizeof(unsigned), length);
		} else {
			leftRes = true;
		}
		if(!leftRes && lowKeyInclusive && lowKeyCompare == 0) {
			return true;
		}
		// get the result of comparison
		int highKeyCompare = 0;
		if(highKey != NULL) {
			highKeyCompare = memcmp((char *) highKey + sizeof(unsigned), (char *) key + sizeof(unsigned), length);
		} else {
			rightRes = true;
		}
		if(!rightRes && highKeyInclusive && highKeyCompare == 0) {
			return true;
		}
		if((leftRes || lowKeyCompare < 0) && (rightRes || highKeyCompare > 0)) {
			return true;
		}
		break;
	}
	default:
		break;
	}
	return false;

}

/**
 * given current ridOffset and ridPointer
 * get current entry
 * strategy guarantee it is valid
 */
RC IX_ScanIterator::findCurEntry(RID &rid, void* key) {
	if(ridPointer >= curKeySlot->numOfRids) {
		cout << "findCurEntry" << endl;
		return -1;
	}
	// get the pageNum
	unsigned offset = curKeySlot->ridOffset + RID_SIZE * ridPointer;
	memcpy(&rid.pageNum, pageDummy + offset, sizeof(unsigned));
	// get the slotNum
	offset += sizeof(unsigned);
	memcpy(&rid.slotNum, pageDummy + offset, sizeof(unsigned));
	memcpy(key, curKeySlot->key, curKeySlot->keyLength);
	return 0;
}

/**
 * given current ridOffset and ridPointer
 * get next entry
 * might need to read new key slot, or new page
 */
RC IX_ScanIterator::findNextEntry(RID &rid, void* key) {
	if(ridPointer >= curKeySlot->numOfRids) {
		cout << "findNextEntry" << endl;
		return -1;
	}
	if(ridPointer + 1 == curKeySlot->numOfRids) {
		if(keySlotList.empty()) {
			if(iterNewPage()) {
				// free memory
				free(curKeySlot->key);
				delete curKeySlot;
				curKeySlot = NULL;
				return IX_EOF;
			}
		} else {
			// free memory
			free(curKeySlot->key);
			delete curKeySlot;
			curKeySlot = (KeySlot*) keySlotList.front();
			keySlotList.pop();
			ridPointer = 0;
		}
	} else {
		ridPointer++;
	}
	return findCurEntry(rid, key);
}


/**
 * close
 * similar to destructor
 */

RC IX_ScanIterator::close() {
//    if(pageDummy!= NULL) {
//        free(pageDummy);
//        pageDummy = NULL;
//    }
//    if(curKeySlot != NULL) {
//        delete curKeySlot;
//        curKeySlot = NULL;
//    }
//    if(preRid != NULL) {
//        delete preRid;
//        preRid = NULL;
//    }
	ridPointer = 0;
	(*preRid).pageNum = 0xffffffff;
    (*preRid).slotNum = 0xffffffff;
	isStart = true;

    return 0;
}

IXFileHandle::IXFileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::openIndexFiles(const string &fileName, PagedFileManager* pfm) {
	RC rc;
	string metadataFileName = fileName + "_metadata";
	string primaryFileName = fileName + "_primary";
	rc = pfm->openFile(metadataFileName.c_str(), metadataFileHandle);
	if (rc) {
		pfm->closeFile(metadataFileHandle);
		cout << "openIndexFiles: error in openFile!" << endl;
		return rc;
	}
	rc = pfm->openFile(primaryFileName.c_str(), primaryFileHandle);
	if (rc) {
		pfm->closeFile(metadataFileHandle);
		pfm->closeFile(primaryFileHandle);
		cout << "openIndexFiles: error in openFile!" << endl;
		return rc;
	}
	return rc;

}

RC IXFileHandle::appendPageToMetadataFile(const void* data) {
	RC rc = metadataFileHandle.appendPage(data);
	if (!rc)
		appendPageCounter++;
	return rc;
}

RC IXFileHandle::readPageFromMetadataFile(PageNum pageNum, void* data) {
	RC rc = metadataFileHandle.readPage(pageNum, data);
	if (!rc)
		readPageCounter++;
	return rc;
}

RC IXFileHandle::writePageToMetadataFile(PageNum pageNum, const void *data) {
	RC rc = metadataFileHandle.writePage(pageNum, data);
	if (!rc)
		writePageCounter++;
	return rc;
}

RC IXFileHandle::appendPageToPrimaryFile(const void* data) {
	RC rc = primaryFileHandle.appendPage(data);
	if (!rc)
		appendPageCounter++;
	return rc;
}

RC IXFileHandle::readPageFromPrimaryFile(PageNum pageNum, void* data) {
	RC rc = primaryFileHandle.readPage(pageNum, data);
	if (!rc)
		readPageCounter++;
	return rc;
}

RC IXFileHandle::writePageToPrimaryFile(PageNum pageNum, const void* data) {
	RC rc = primaryFileHandle.writePage(pageNum, data);
	if (!rc)
		writePageCounter++;
	return rc;
}

RC IXFileHandle::closeIndexFiles(PagedFileManager *pfm) {
	RC rc = pfm->closeFile(metadataFileHandle);
	if (rc) {
		cout << "closeIndexFiles: error in closeFile!" << endl;
		return rc;
	}
	rc = pfm->closeFile(primaryFileHandle);
	if (rc) {
		cout << "closeIndexFiles: error in closeFile!" << endl;
		return rc;
	}
	return rc;
}

unsigned IXFileHandle::getNumOfPagesInMetadataFile()
{
	return metadataFileHandle.getNumberOfPages();
}

unsigned IXFileHandle::getNumOfPagesInPrimaryFile()
{
	return primaryFileHandle.getNumberOfPages();
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCount = readPageCounter;
	writePageCount = writePageCounter;
	appendPageCount = appendPageCounter;
	return 0;
}



void IX_PrintError (RC rc)
{
}
