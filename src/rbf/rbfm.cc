
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
