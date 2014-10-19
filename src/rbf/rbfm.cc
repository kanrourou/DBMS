
#include "rbfm.h"
#include <iostream>


RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
	if(!_rbf_manager)
		_rbf_manager = new RecordBasedFileManager();

	return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
	pfm = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
	return pfm->createFile(fileName.c_str());
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return pfm->destroyFile(fileName.c_str());
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
	return pfm->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
	return pfm->closeFile(fileHandle);
}

int RecordBasedFileManager::lookForPageInDir(FileHandle &fileHandle, int recordSize)
{
	int numOfPages = fileHandle.getNumberOfPages();
	int numOfDirs = (numOfPages / FREE_SPACE_DIR_SIZE) + 1;
	void* dir = malloc(PAGE_SIZE);
	for (int i = 0; i < numOfDirs; i++)
	{
		//read directory
		fileHandle.readPage(i * FREE_SPACE_DIR_SIZE, dir);
		int numOfPagesInDir = 0;
		memcpy(&numOfPagesInDir, dir, sizeof(int));
		for (int j = 1; j <= numOfPagesInDir; j++)
		{
			int freeSpace = 0;
			memcpy(&freeSpace, (int*)dir + j, sizeof(int));
			if (freeSpace >= recordSize + SLOT_SIZE)
				return i * FREE_SPACE_DIR_SIZE + j;
		}
	}
	free(dir);
	return -1;
}

int RecordBasedFileManager::getPage(FileHandle &fileHandle, void *page, bool& isNew, int recordSize, int& freePageNum) 
{
		//create directory when necessary
		if (!(fileHandle.getNumberOfPages() % FREE_SPACE_DIR_SIZE))
		{
			void* dir = malloc(PAGE_SIZE);
			int dirSize = 0;
			memcpy(dir, &dirSize, sizeof(int));
			fileHandle.appendPage(dir);
			free(dir);

		}
		//if it is the first page
		//if we need to append new page, since previous page cannot hold the record
		bool findAvailablePage = false;
		int pageNum = lookForPageInDir(fileHandle, recordSize);
		findAvailablePage = pageNum == -1? false: true;
		//if there is no available page, append new page, updtae directory
		if (!findAvailablePage)
		{
			//append new page
			fileHandle.appendPage(page);
			//get new page num
			pageNum = fileHandle.getNumberOfPages() - 1;
			void* dir = malloc(PAGE_SIZE);
			//calculate directory num
			int dirNum = pageNum / FREE_SPACE_DIR_SIZE;
			//read directory
			fileHandle.readPage(dirNum * FREE_SPACE_DIR_SIZE, dir);
			//calculate page's offset in directory
			int pageOffsetInDir = pageNum % FREE_SPACE_DIR_SIZE;
			int freeSpace = PAGE_SIZE - 8;//last two bytes for freeSpaceOffset and numOfSlots
			//set free space for that page in directory
			memcpy((int*)dir + pageOffsetInDir, &freeSpace, sizeof(int));
			//update num of pages in directory
			int numOfPagesInDir = 0;
			memcpy(&numOfPagesInDir, dir, sizeof(int));
			numOfPagesInDir++;
			memcpy(dir, &numOfPagesInDir, sizeof(int));
			fileHandle.writePage(dirNum * FREE_SPACE_DIR_SIZE, dir);
			free(dir);
			isNew = true;
		}

		//get that page
		freePageNum = pageNum;
		return fileHandle.readPage(freePageNum, page);

}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	int recordSize = 0,
			slotOffset = 0,
			strLen = 0,
			freeSpaceOffset = 0,
			freePageNum = 0;
	size_t vecLen = recordDescriptor.size();
	bool isNew = false;//if the page has been initialized
	//calculate recordSize
	for (size_t i = 0; i < vecLen; i++)
	{
		switch(recordDescriptor[i].type)
		{
		case TypeInt:
			recordSize += sizeof(int);
			break;
		case TypeReal:
			recordSize += sizeof(float);
			break;
		case TypeVarChar:
			memcpy(&strLen,(char*)data + recordSize,sizeof(int));
			recordSize += (strLen + sizeof(int));
			break;
		}
	}
	void* page = malloc(PAGE_SIZE);
	//get available page
	int ret = getPage(fileHandle, page, isNew, recordSize, freePageNum);
	//if it is a new allocated page
	if(isNew)
		initializePage(page);//initialize new allocated page

	//if page can hold this record
	freeSpaceOffset = getFreeSpaceOffset(page);//get free space offset in the page
	//insert record
	memcpy((char*)page + freeSpaceOffset, data, recordSize);
	slotOffset = getNextSlotOffset(page);
	//insert slot
	memcpy((char*)page + slotOffset, &freeSpaceOffset, sizeof(int));//insert record start position
	memcpy((char*)page + slotOffset + sizeof(int),&recordSize, sizeof(int));//insert record length
	//update free space offset
	updateFreeSpaceOffset(page, freeSpaceOffset + recordSize);
	//update num of slots
	int numOfSlots = getNumOfSlots(page);
	numOfSlots++;
	updateNumOfSlots(page, numOfSlots);
	//update directory
	int dirNum = freePageNum / FREE_SPACE_DIR_SIZE;
	int pageOffsetInDir = freePageNum % FREE_SPACE_DIR_SIZE;
	void* dir = malloc(PAGE_SIZE);
	fileHandle.readPage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	int freeSpace = 0;
	memcpy(&freeSpace, (int*)dir + pageOffsetInDir, sizeof(int));
	freeSpace -= (recordSize + SLOT_SIZE);
	memcpy((int*)dir + pageOffsetInDir, &freeSpace, sizeof(int));
	//dump directory
	fileHandle.writePage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	free(dir);
	//dump page
	fileHandle.writePage(freePageNum, page);
	//set rid
	rid.pageNum = freePageNum;
	rid.slotNum = numOfSlots - 1;
	free(page);
	return ret;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	int recordOffset = 0,
			recordLen = 0;
	void* page = malloc(PAGE_SIZE);
	//read page according to page number
	fileHandle.readPage(rid.pageNum, page);
	//get offset of that slot number
	getSlot(recordOffset, recordLen, rid.slotNum, page);
	memcpy(data, (char*)page + recordOffset, recordLen);
	free(page);
	return 0;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int recordSize = 0, strLen = 0;
	size_t vecLen = recordDescriptor.size();
	int valInt = 0;
	float valFloat = 0;
	char* varChar;
	for (size_t i = 0; i < vecLen; i++)
	{
		switch(recordDescriptor[i].type)
		{
		case TypeInt:
			memcpy(&valInt, (char*)data + recordSize, sizeof(int));
			std::cout << valInt;
			recordSize += sizeof(int);
			break;
		case TypeReal:
			memcpy(&valFloat, (char*)data + recordSize, sizeof(float));
			std::cout << valFloat;
			recordSize += sizeof(float);
			break;
		case TypeVarChar:
			memcpy(&strLen,(char*)data + recordSize,sizeof(int));
			recordSize += sizeof(int);
			varChar = (char*)malloc(strLen);//allocate memory according to string length
			memcpy(varChar, (char*)data + recordSize, strLen);
			recordSize += strLen;
			std::cout << varChar;
			free(varChar);
			break;
		}
		if (i != vecLen - 1)
			std::cout <<",";
	}
	std::cout << std::endl;
	return 0;
}
