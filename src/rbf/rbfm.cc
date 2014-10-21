
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
			{
				free(dir);
				return i * FREE_SPACE_DIR_SIZE + j;

			}
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

//append data with field information
void RecordBasedFileManager::prepareRecord(const vector<Attribute> &recordDescriptor, void* appendedData, const void* data)
{
	int recordSize = 0, 
		numOfField = recordDescriptor.size(), 
		recordFlag = 1,
		type = 0,
		strLen = 0;
	size_t vecLen = recordDescriptor.size();
	int appendedDataSize = 2 * sizeof(int) + FIELD_INFO_SIZE * numOfField;
	int fieldOffset = appendedDataSize;
	memcpy(appendedData, &recordFlag, sizeof(int));
	memcpy((char*)appendedData + sizeof(int), &numOfField, sizeof(int));
	for (size_t i = 0; i < vecLen; i++)
	{
		switch(recordDescriptor[i].type)
		{
		case TypeInt:
			type = 0;
			memcpy((char*)appendedData + 2 * sizeof(int) + i * FIELD_INFO_SIZE, &fieldOffset, sizeof(int));
			memcpy((char*)appendedData + 2 * sizeof(int) + i * FIELD_INFO_SIZE + sizeof(int), &type, sizeof(int));
			recordSize += sizeof(int);
			fieldOffset += sizeof(int);
			break;
		case TypeReal:
			type = 1;
			memcpy((char*)appendedData + 2 * sizeof(int) + i * FIELD_INFO_SIZE, &fieldOffset, sizeof(int));
			memcpy((char*)appendedData + 2 * sizeof(int) + i * FIELD_INFO_SIZE + sizeof(int), &type, sizeof(int));
			recordSize += sizeof(float);
			fieldOffset += sizeof(float);
			break;
		case TypeVarChar:
			type = 2;
			memcpy((char*)appendedData + 2 * sizeof(int) + i * FIELD_INFO_SIZE, &fieldOffset, sizeof(int));
			memcpy((char*)appendedData + 2 * sizeof(int) + i * FIELD_INFO_SIZE + sizeof(int), &type, sizeof(int));
			memcpy(&strLen,(char*)data + recordSize,sizeof(int));
			recordSize += (strLen + sizeof(int));
			fieldOffset += (strLen + sizeof(int));
			break;
		}
	}
	memcpy((char*)appendedData + appendedDataSize, data, recordSize);

}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	int recordSize = 0,
			slotOffset = 0,
			strLen = 0,
			freeSpaceOffset = 0,
			freePageNum = 0,
			appendedDataSize = 0;
	int numOfField = (int)recordDescriptor.size();
	appendedDataSize = 2 * sizeof(int) + FIELD_INFO_SIZE * numOfField;//flag + num of fields + num of fields * (field offset + field type)
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
	//total size of single record
	recordSize += appendedDataSize;
	void* appendedData = malloc(recordSize);
	//append record with field info
	prepareRecord(recordDescriptor, appendedData, data);
	void* page = malloc(PAGE_SIZE);
	//get available page
	int ret = getPage(fileHandle, page, isNew, recordSize, freePageNum);
	//if it is a new allocated page
	if(isNew)
		initializePage(page);//initialize new allocated page

	//if page can hold this record
	freeSpaceOffset = getFreeSpaceOffset(page);//get free space offset in the page
	//insert record
	memcpy((char*)page + freeSpaceOffset, appendedData, recordSize);
	free(appendedData);
	slotOffset = getAvailableSlotOffset(page);
	//insert slot
	int slotFlag = 1;
	memcpy((char*)page + slotOffset, &slotFlag, sizeof(int));//insert record start position
	memcpy((char*)page + slotOffset + sizeof(int),&freeSpaceOffset, sizeof(int));//insert record length
	memcpy((char*)page + slotOffset + 2 * sizeof(int),&recordSize, sizeof(int));//insert record length

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

	RC res = -1;
	RID realRid = {rid.pageNum, rid.slotNum};
	RID temp = {rid.pageNum, rid.slotNum};
	while(res) {
		res = getRealRecordRid(fileHandle, temp, realRid, (char *)data);
		if(!res) return 0;
		temp.pageNum = realRid.pageNum;
		temp.slotNum = realRid.slotNum;

	}
	return -1;
}


RC RecordBasedFileManager::getRealRecordRid(FileHandle &fileHandle, const RID &rid, RID &realRid, char* data) {
	if(fileHandle.getFile()==NULL) return -1;
	char* page_dummy = (char*)malloc(PAGE_SIZE);
	if(page_dummy==NULL) return -1;
	if(fileHandle.readPage(rid.pageNum, page_dummy))
	{
		freeMemory(page_dummy);
		return -1;
	}
	// get the record slot information
	int record_offset = 0;
	int record_length = 0;
	getSlot(record_offset, record_length, rid.slotNum, page_dummy);
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, page_dummy+record_offset, sizeof(int));
	if(record_flag) {
		realRid.pageNum = rid.pageNum;
		realRid.slotNum = rid.slotNum;
		if(getRealRecord(realRid, page_dummy, data))
		{
			freeMemory(page_dummy);
			return -1;
		}
		freeMemory(page_dummy);
		return 0;
	} else {
		if(record_length != sizeof(int)*3)
		{
			cout << "there is a problem about record's RID length, it != 12" << endl;
		}
		record_offset += sizeof(int);
		memcpy(&realRid.pageNum, page_dummy+record_offset, sizeof(unsigned));
		record_offset += sizeof(unsigned);
		memcpy(&realRid.slotNum, page_dummy+record_offset, sizeof(unsigned));
	}

	freeMemory(page_dummy);
	if(record_flag) return 0;
	else return -1;
}

RC RecordBasedFileManager::freeMemory(char* page_dummy) {
	if(page_dummy==NULL) return -1;
	free(page_dummy);
	page_dummy == NULL;
	return 0;
}

RC RecordBasedFileManager::getRealRecord(const RID &rid, char* page_dummy, char* data)
{
	// get the record slot information
	int record_offset = 0;
	int record_length = 0;
	getSlot(record_offset, record_length, rid.slotNum, page_dummy);
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, page_dummy+record_offset, sizeof(int));
	if(!record_flag) return -1;
	record_offset += sizeof(int) * 2;
	int real_record_offset = 0;
	memcpy(&real_record_offset, page_dummy+record_offset, sizeof(int));
	int real_record_length = record_length - real_record_offset;
	record_offset -= 2 * sizeof(int);
	memcpy(data, page_dummy+record_offset+real_record_offset, real_record_length);
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

RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle)
{
	return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid)
{
	RC res = -1;
	RID realRid = {rid.pageNum, rid.slotNum};
	RID temp = {rid.pageNum, rid.slotNum};
	while(res) {
		res = deleteRecordHelper(fileHandle, temp, realRid);
		if(!res) return 0;
		temp.pageNum = realRid.pageNum;
		temp.slotNum = realRid.slotNum;
	}
	return -1;
}

RC  RecordBasedFileManager::deleteRecordHelper(FileHandle &fileHandle, const RID &rid, RID &realRid)
{
	if (fileHandle.getFile() == NULL) return -1;
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL) return -1;
	if (fileHandle.readPage(rid.pageNum, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	// get the record slot information
	int record_offset = 0;
	int record_length = 0;
	getSlot(record_offset, record_length, rid.slotNum, page_dummy);
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, page_dummy + record_offset, sizeof(int));
	if (!record_flag) { // if content is RID
		if (record_length != sizeof(int) * 3) {
			cout << "there is a problem about record's RID length, it != 12" << endl;
		}
		record_offset += sizeof(int);
		memcpy(&realRid.pageNum, page_dummy + record_offset, sizeof(unsigned));
		record_offset += sizeof(unsigned);
		memcpy(&realRid.slotNum, page_dummy + record_offset, sizeof(unsigned));
	}
	// update info in current page
	int slot_offset = getSlotOffset(rid.slotNum);
	int delete_flag = 0;
	memcpy(page_dummy+slot_offset, &delete_flag, sizeof(int));
	if(fileHandle.writePage(rid.pageNum, page_dummy))
	{
		freeMemory(page_dummy);
		return -1;
	}
	// update the free space directory
	int freespace_dir_page_num = rid.pageNum / FREE_SPACE_DIR_SIZE * FREE_SPACE_DIR_SIZE;
	if(fileHandle.readPage(freespace_dir_page_num, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	int freespace_dir_offset = rid.pageNum % FREE_SPACE_DIR_SIZE * sizeof(int);
	int freespace = 0;
	memcpy(&freespace, page_dummy+freespace_dir_offset, sizeof(int));
	freespace += record_length;
	if(freespace>PAGE_SIZE) {
		cout << "there is a problem about freesapce" << endl;
	}
	memcpy(page_dummy+freespace_dir_offset, &freespace, sizeof(int));
	if(fileHandle.writePage(freespace_dir_page_num, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(page_dummy);
	if(record_flag) return 0;
	return -1;
}



