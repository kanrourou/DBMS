
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
	delete _rbf_manager;
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

//search available page in free space directory
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
				dir = 0;
				return i * FREE_SPACE_DIR_SIZE + j;

			}
		}
	}
	free(dir);
	dir = 0;
	return -1;
}

//return the page to be inserted
int RecordBasedFileManager::getPage(FileHandle &fileHandle, void *page, const vector<Attribute> &recordDescriptor, int recordSize, int& freePageNum) 
{
	//create directory when necessary
	if (!(fileHandle.getNumberOfPages() % FREE_SPACE_DIR_SIZE))
	{
		void* dir = malloc(PAGE_SIZE);
		int dirSize = 0;
		memcpy(dir, &dirSize, sizeof(int));
		fileHandle.appendPage(dir);
		free(dir);
		dir = 0;

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
		dir = 0;
	}
	else
	{
		reorganizePage(fileHandle, recordDescriptor, pageNum);

	}
	//get that page
	freePageNum = pageNum;
	return fileHandle.readPage(freePageNum, page);
}

//return the availabe slot offset
int RecordBasedFileManager::getAvailableSlotOffset(void* page, bool& isNewSlot, int& slotNum)
{
	int numOfSlots = getNumOfSlots(page);
	for (int i = 0; i < numOfSlots; i++)
	{
		if(isTomb(page, i))
		{
			isNewSlot = false;
			slotNum = i;
			return getSlotOffset(i);

		}
	}
	isNewSlot = true;
	slotNum = numOfSlots;
	return PAGE_SIZE - (numOfSlots + 1) * SLOT_SIZE - 2 * sizeof(int);
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

//insert record
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
	int recordSize = 0,
			slotOffset = 0,
			strLen = 0,
			freeSpaceOffset = 0,
			freePageNum = 0,
			appendedDataSize = 0, 
			slotNum = 0;
	int numOfField = (int)recordDescriptor.size();
	bool isNewSlot = true;
	appendedDataSize = 2 * sizeof(int) + FIELD_INFO_SIZE * numOfField;//flag + num of fields + num of fields * (field offset + field type)
	size_t vecLen = recordDescriptor.size();
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
	//initialize page
	initializePage(page);
	//get available page
	int ret = getPage(fileHandle, page, recordDescriptor, recordSize, freePageNum);
	freeSpaceOffset = getFreeSpaceOffset(page);//get free space offset in the page
	//insert record
	memcpy((char*)page + freeSpaceOffset, appendedData, recordSize);
	free(appendedData);
	appendedData = 0;
	slotOffset = getAvailableSlotOffset(page, isNewSlot, slotNum);
	//insert slot
	int slotFlag = 1;
	memcpy((char*)page + slotOffset, &slotFlag, sizeof(int));//insert record start position
	memcpy((char*)page + slotOffset + sizeof(int),&freeSpaceOffset, sizeof(int));//insert record offset
	memcpy((char*)page + slotOffset + 2 * sizeof(int),&recordSize, sizeof(int));//insert record length

	//update free space offset
	updateFreeSpaceOffset(page, freeSpaceOffset + recordSize);
	//if we need to update num of slots, num of slots includes tombs
	if (isNewSlot)
	{
		int numOfSlots = getNumOfSlots(page);
		numOfSlots++;
		updateNumOfSlots(page, numOfSlots);

	}
	//update directory
	int dirNum = freePageNum / FREE_SPACE_DIR_SIZE;
	int pageOffsetInDir = freePageNum % FREE_SPACE_DIR_SIZE;
	void* dir = malloc(PAGE_SIZE);
	fileHandle.readPage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	int freeSpace = 0;
	memcpy(&freeSpace, (int*)dir + pageOffsetInDir, sizeof(int));
	freeSpace -= isNewSlot?(recordSize + SLOT_SIZE): recordSize;
	memcpy((int*)dir + pageOffsetInDir, &freeSpace, sizeof(int));
	//dump directory
	fileHandle.writePage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	free(dir);
	dir = 0;
	//dump page
	fileHandle.writePage(freePageNum, page);
	//set rid
	rid.pageNum = freePageNum;
	rid.slotNum = slotNum;
	free(page);
	page = 0;
	return ret;
}

//insert record at given page
RC RecordBasedFileManager::insertRecordAtPage(FileHandle& fileHandle, void* page, const vector<Attribute> &recordDescriptor,
		const void *data, const RID& rid, int oldRecordLen)
{
	int recordSize = 0,
			slotOffset = 0,
			strLen = 0,
			freeSpaceOffset = 0,
			appendedDataSize = 0;
	int numOfField = (int)recordDescriptor.size();
	appendedDataSize = 2 * sizeof(int) + FIELD_INFO_SIZE * numOfField;//flag + num of fields + num of fields * (field offset + field type)
	size_t vecLen = recordDescriptor.size();
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
	freeSpaceOffset = getFreeSpaceOffset(page);//get free space offset in the page
	//insert record
	memcpy((char*)page + freeSpaceOffset, appendedData, recordSize);
	free(appendedData);
	appendedData = 0;
	slotOffset = getSlotOffset(rid.slotNum);
	//insert slot
	int slotFlag = 1;
	memcpy((char*)page + slotOffset, &slotFlag, sizeof(int));//insert record flag
	memcpy((char*)page + slotOffset + sizeof(int),&freeSpaceOffset, sizeof(int));//insert record offset
	memcpy((char*)page + slotOffset + 2 * sizeof(int),&recordSize, sizeof(int));//insert record length

	//update free space offset
	updateFreeSpaceOffset(page, freeSpaceOffset + recordSize);
	//update directory
	int dirNum = rid.pageNum / FREE_SPACE_DIR_SIZE;
	int pageOffsetInDir = rid.pageNum % FREE_SPACE_DIR_SIZE;
	void* dir = malloc(PAGE_SIZE);
	fileHandle.readPage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	int freeSpace = 0;
	memcpy(&freeSpace, (int*)dir + pageOffsetInDir, sizeof(int));
	freeSpace -= (recordSize - oldRecordLen);
	memcpy((int*)dir + pageOffsetInDir, &freeSpace, sizeof(int));
	//dump directory
	fileHandle.writePage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	free(dir);
	dir = 0;
	//dump page
	fileHandle.writePage(rid.pageNum, page);
	return 0;
}

RC RecordBasedFileManager::insertRidRecordAtPage(FileHandle& fileHandle, void* page, const void *ridRecord,
		const RID& rid, int oldRecordLen)
{
	int recordSize = RID_RECORD_SIZE,
			slotOffset = 0,
			freeSpaceOffset = 0;
	freeSpaceOffset = getFreeSpaceOffset(page);//get free space offset in the page
	//insert rid record
	memcpy((char*)page + freeSpaceOffset, ridRecord, RID_RECORD_SIZE);
	slotOffset = getSlotOffset(rid.slotNum);
	//insert slot
	int slotFlag = 1;
	memcpy((char*)page + slotOffset, &slotFlag, sizeof(int));//insert record start position
	memcpy((char*)page + slotOffset + sizeof(int),&freeSpaceOffset, sizeof(int));//insert record offset
	memcpy((char*)page + slotOffset + 2 * sizeof(int),&recordSize, sizeof(int));//insert record length

	//update free space offset
	updateFreeSpaceOffset(page, freeSpaceOffset + recordSize);
	//update directory
	int dirNum = rid.pageNum / FREE_SPACE_DIR_SIZE;
	int pageOffsetInDir = rid.pageNum % FREE_SPACE_DIR_SIZE;
	void* dir = malloc(PAGE_SIZE);
	fileHandle.readPage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	int freeSpace = 0;
	memcpy(&freeSpace, (int*)dir + pageOffsetInDir, sizeof(int));
	freeSpace -= (recordSize - oldRecordLen);
	memcpy((int*)dir + pageOffsetInDir, &freeSpace, sizeof(int));
	//dump directory
	fileHandle.writePage(dirNum * FREE_SPACE_DIR_SIZE, dir);
	free(dir);
	dir = 0;
	//dump page
	fileHandle.writePage(rid.pageNum, page);
	return 0;
}


//prepare rid record to be inserted
void RecordBasedFileManager::prepareRid(void* ridRecord, const RID &rid)
{
	int recordFlag = 0;
	memcpy(ridRecord, &recordFlag, sizeof(int));
	memcpy((char*)ridRecord + sizeof(int), &(rid.pageNum), sizeof(int));
	memcpy((char*)ridRecord + 2 * sizeof(int), &(rid.slotNum), sizeof(int));
	return;
}

//update record
RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
	int recordSize = 0;
	int strLen = 0;
	size_t vecLen = recordDescriptor.size();
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

	recursivelyLookingForAndUpdate(fileHandle, recordDescriptor, data, rid, recordSize);
	return 0;
}

//recursively find the record and update it 
void RecordBasedFileManager::recursivelyLookingForAndUpdate(FileHandle& fileHandle, const vector<Attribute> &recordDescriptor,
		const void* data, const RID &rid, int recordSize)
{
	int recordOffset = 0,
			recordLen = 0;
	void* page = malloc(PAGE_SIZE);
	fileHandle.readPage(rid.pageNum, page);
	getSlot(recordOffset, recordLen, rid.slotNum, page);
	if (isRecord(page, recordOffset))
	{
		createTomb(page, rid.slotNum);
		fileHandle.writePage(rid.pageNum, page);
		reorganizePage(fileHandle, recordDescriptor, rid.pageNum);
		fileHandle.readPage(rid.pageNum, page);
		int freeSpace = getFreeSpace(page);
		int numOfField = recordDescriptor.size();
		int appendedDataSize = 2 * sizeof(int) + FIELD_INFO_SIZE * numOfField;//flag + num of fields + num of fields * (field offset + field type)
		recordSize += appendedDataSize;
		if (freeSpace >= recordSize)
		{
			insertRecordAtPage(fileHandle, page, recordDescriptor, data, rid, recordLen);

		}
		else 
		{
			RID newRid = {0, 0};
			insertRecord(fileHandle, recordDescriptor, data, newRid);
			void* ridRecord = malloc(RID_RECORD_SIZE);
			prepareRid(ridRecord, newRid);
			insertRidRecordAtPage(fileHandle, page, ridRecord, rid, recordLen);
			free(ridRecord);
			ridRecord = 0;
		}
		fileHandle.writePage(rid.pageNum, page);
		free(page);
		page = 0;
	}
	else
	{
		RID newRid = {0, 0};
		getRidInPage(page, recordOffset, newRid);
		free(page);
		page = 0;
		recursivelyLookingForAndUpdate(fileHandle, recordDescriptor, data, newRid, recordSize);
	}

	return;
}


/**
 * free memory allocated by malloc()
 */
RC RecordBasedFileManager::freeMemory(char* page_dummy) {
	if (page_dummy == NULL)
		return -1;
	free(page_dummy);
	page_dummy = NULL;
	return 0;
}
/**
 * get a record given by the RID
 * the recordDescriptor is in no use here
 */
RC RecordBasedFileManager::readRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
	// check whether it is a tomb
	if (fileHandle.getFile() == NULL) return -1;
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL) return -1;
	if (fileHandle.readPage(rid.pageNum, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	// need to check whether the record is deleted
	int offset = PAGE_SIZE - sizeof(int) * 2 - sizeof(int) * 3 * (rid.slotNum + 1);
	int record_flag = 0;
	memcpy(&record_flag, page_dummy+offset, sizeof(int));
	if(record_flag==0) {
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(page_dummy);
	// start the get the record
	RC res = -1;
	RID realRid = { rid.pageNum, rid.slotNum };
	RID temp = { rid.pageNum, rid.slotNum };
	while (res) {
		res = getRealRecordRid(fileHandle, temp, realRid, (char*) data);
		if (!res) return 0;
		temp.pageNum = realRid.pageNum;
		temp.slotNum = realRid.slotNum;
	}
	return -1;
}
/**
 * given the RID get the content, identify whether the content is record
 * -1 - content is RID
 * 0 - content is record, and it will be stored in char* data
 */
RC RecordBasedFileManager::getRealRecordRid(FileHandle &fileHandle,
		const RID &rid, RID &realRid, char* data) {
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
	// if is record
	if (record_flag) {
		realRid.pageNum = rid.pageNum;
		realRid.slotNum = rid.slotNum;
		if (getRealRecord(realRid, page_dummy, data)) {
			freeMemory(page_dummy);
			return -1;
		}
		freeMemory(page_dummy);
		return 0;
		// if is RID
	} else {
		if (record_length != sizeof(int) * 3) {
			cout << "there is a problem about record's RID length, it != 12" << endl;
		}
		// get rid's page number
		record_offset += sizeof(int);
		memcpy(&(realRid.pageNum), page_dummy + record_offset, sizeof(unsigned));
		// get rid's slot number
		record_offset += sizeof(unsigned);
		memcpy(&(realRid.slotNum), page_dummy + record_offset, sizeof(unsigned));
	}

	freeMemory(page_dummy);
	if (record_flag) return 0;
	else return -1;
}

/**
 * get the record given the rid and page
 */
RC RecordBasedFileManager::getRealRecord(const RID &rid, char* page_dummy, char* data) {
	// get the record slot information
	int record_offset = 0;
	int record_length = 0;
	getSlot(record_offset, record_length, rid.slotNum, page_dummy);
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, page_dummy + record_offset, sizeof(int));
	if (!record_flag) return -1;
	record_offset += sizeof(int) * 2;
	int real_record_offset = 0;
	memcpy(&real_record_offset, page_dummy + record_offset, sizeof(int));
	record_offset -= sizeof(int) * 2;
	int real_record_length = record_length - real_record_offset;
	memcpy(data, page_dummy + record_offset + real_record_offset, real_record_length);
	return 0;
}
/**
 * given the recordDescriptor and data, describe the data
 */
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
	int recordSize = 0, strLen = 0;
	size_t vecLen = recordDescriptor.size();
	int valInt = 0;
	float valFloat = 0;
	char* varChar;
	for (size_t i = 0; i < vecLen; i++) {
		switch (recordDescriptor[i].type) {
		case TypeInt:
			memcpy(&valInt, (char*) data + recordSize, sizeof(int));
			std::cout << valInt;
			recordSize += sizeof(int);
			break;
		case TypeReal:
			memcpy(&valFloat, (char*) data + recordSize, sizeof(float));
			std::cout << valFloat;
			recordSize += sizeof(float);
			break;
		case TypeVarChar:
			memcpy(&strLen, (char*) data + recordSize, sizeof(int));
			recordSize += sizeof(int);
			varChar = (char*) malloc(strLen + 1); //allocate memory according to string length
			memcpy(varChar, (char*) data + recordSize, strLen);
			char tail = '\0';
			memcpy(varChar + strLen, &tail, sizeof(char));
			recordSize += strLen;
			std::cout << varChar;
			free(varChar);
			break;
		}
		if (i != vecLen - 1)
			std::cout << ",";
	}
	std::cout << std::endl;
	return 0;
}
/*
 * delete all the records in the file
 * but keep all the pages
 */
RC RecordBasedFileManager::deleteRecords(FileHandle &fileHandle) {
	if (fileHandle.getFile() == NULL) return -1;
	int page_num = fileHandle.getNumberOfPages();
	for (int i = 0; i < page_num; i++) {
		if (i % FREE_SPACE_DIR_SIZE == 0) {
			// clear the free space directory page
			if (clearFreespaceDirPage(fileHandle, i)) return -1;
		} else {
			// clear the record page
			if (clearRecordPage(fileHandle, i)) return -1;
		}
	}
	return 0;
}
/**
 * clear the free space directory page
 * modify the number of page to 0
 */
RC RecordBasedFileManager::clearFreespaceDirPage(FileHandle &fileHandle, int page_num) {
	if (fileHandle.getFile() == NULL) return -1;
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL) return -1;
	if (fileHandle.readPage(page_num, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	int value = 0;
	memcpy(page_dummy, &value, sizeof(int));
	if (fileHandle.writePage(page_num, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(page_dummy);
	return 0;
}
/**
 * clear the record page
 * modify the free space offset to 0
 * modify the number of slot to 0
 */
RC RecordBasedFileManager::clearRecordPage(FileHandle &fileHandle, int page_num) {
	if (fileHandle.getFile() == NULL) return -1;
		char* page_dummy = (char*) malloc(PAGE_SIZE);
		if (page_dummy == NULL) return -1;
		if (fileHandle.readPage(page_num, page_dummy)) {
			freeMemory(page_dummy);
			return -1;
		}
		int value = 0;
		// modify the free space offset
		int page_offset = PAGE_SIZE - sizeof(int);
		memcpy(page_dummy + page_offset, &value, sizeof(int));
		// modify the number of slots
		int num_of_slot = 0;
		page_offset -= sizeof(int);
		memcpy(&num_of_slot, page_dummy + page_offset, sizeof(int));
		memcpy(page_dummy + page_offset, &value, sizeof(int));
		// modify the slots
		for(int i=0;i<num_of_slot;i++) {
			page_offset -= sizeof(int) * 3;
			memcpy(page_dummy + page_offset, &value, sizeof(int));
		}
		if (fileHandle.writePage(page_num, page_dummy)) {
			freeMemory(page_dummy);
			return -1;
		}
		freeMemory(page_dummy);
		return 0;
}

/**
 * delete record chain given a rid
 * trace the rid chain until it find the real record
 */
RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid) {
	// check whether it is a valid rid
	if (fileHandle.getFile() == NULL) {
		cout << "deleteRecord: no associated file!" << endl;
		return -1;
	}
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL){
		cout << "deleteRecord: malloc error!" << endl;
		return -1;
	}
	if (fileHandle.readPage(rid.pageNum, page_dummy)) {
		cout << "deleteRecord: readPage error!" << endl;
		freeMemory(page_dummy);
		return -1;
	}
	int offset = PAGE_SIZE - sizeof(int) * 2;
	int slot_num = 0;
	memcpy(&slot_num, page_dummy+offset, sizeof(int));
	if(rid.slotNum>=slot_num) {
		cout << "deleteRecord: slotNum error!" << endl;
		freeMemory(page_dummy);
		return -1;
	}
	// check whether it is a tomb
	offset -= sizeof(int) * 3 * (rid.slotNum + 1);
	int record_flag = 0;
	memcpy(&record_flag, page_dummy+offset, sizeof(int));
	if(record_flag==0) {
		cout << "deleteRecord: try to delete tomb!" << endl;
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(page_dummy);
	// start to delete
	RC res = -1;
	RID realRid = { rid.pageNum, rid.slotNum };
	RID temp = { rid.pageNum, rid.slotNum };
	while (res) {
		res = deleteRecordHelper(fileHandle, temp, realRid);
		if (!res) return 0;
		temp.pageNum = realRid.pageNum;
		temp.slotNum = realRid.slotNum;
	}
	cout << "deleteRecord: error in looking for record!" << endl;
	return -1;
}

/**
 * delete record given a rid
 * 0 - indicate the given rid point to a real record
 * -1 - indicate the given rid point to another rid
 */
RC RecordBasedFileManager::deleteRecordHelper(FileHandle &fileHandle,
		const RID &rid, RID &realRid) {
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
		memcpy(&(realRid.pageNum), page_dummy + record_offset, sizeof(unsigned));
		record_offset += sizeof(unsigned);
		memcpy(&(realRid.slotNum), page_dummy + record_offset, sizeof(unsigned));
	}
	// update info in current page
	int slot_offset = getSlotOffset(rid.slotNum);
	int delete_flag = 0;
	memcpy(page_dummy + slot_offset, &delete_flag, sizeof(int));
	if (fileHandle.writePage(rid.pageNum, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	// update the free space directory
	int freespace_dir_page_num = rid.pageNum / FREE_SPACE_DIR_SIZE * FREE_SPACE_DIR_SIZE;
	if (fileHandle.readPage(freespace_dir_page_num, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	int freespace_dir_offset = rid.pageNum % FREE_SPACE_DIR_SIZE * sizeof(int);
	int freespace = 0;
	memcpy(&freespace, page_dummy + freespace_dir_offset, sizeof(int));
	freespace += record_length;
	if (freespace > PAGE_SIZE) {
		cout << "there is a problem about freespace" << endl;
	}
	memcpy(page_dummy + freespace_dir_offset, &freespace, sizeof(int));
	if (fileHandle.writePage(freespace_dir_page_num, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(page_dummy);
	if (record_flag) return 0;
	return -1;
}

/**
 * trace the rid chain to get the final record's attribute
 * O(1) time complexity
 */
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const RID &rid,
		const string attributeName, void *data) {
	if (fileHandle.getFile() == NULL) return -1;
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL) return -1;
	if (fileHandle.readPage(rid.pageNum, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	// need to check whether the record is deleted
	int offset = PAGE_SIZE - sizeof(int) * 2 - sizeof(int) * 3 * (rid.slotNum + 1);
	int record_flag = 0;
	memcpy(&record_flag, page_dummy+offset, sizeof(int));
	if(record_flag==0) {
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(page_dummy);
	// get the attribute index
	int idx = 0;
	for (; idx < recordDescriptor.size(); idx++) {
		Attribute tempAtt = recordDescriptor[idx];
		if (tempAtt.name == attributeName)
			break;
	}
	// get the attribute
	RC res = -1;
	RID realRid = { rid.pageNum, rid.slotNum };
	RID temp = { rid.pageNum, rid.slotNum };
	while (res) {
		res = getRealRecordAttributeRid(fileHandle, temp, realRid, idx, (char*) data);
		if (!res) return 0;
		temp.pageNum = realRid.pageNum;
		temp.slotNum = realRid.slotNum;
	}
	return -1;
}

/**
 * get attribute given the rid
 * 0 - the rid points to the real record
 * -1 - the rid points to another rid
 */
RC RecordBasedFileManager::getRealRecordAttributeRid(FileHandle &fileHandle,
		const RID &rid, RID &realRid, int idx, char* data) {
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
	// if is record
	if (record_flag) {
		realRid.pageNum = rid.pageNum;
		realRid.slotNum = rid.slotNum;
		if (getRealRecordAttribute(realRid, page_dummy, idx, data)) {
			freeMemory(page_dummy);
			return -1;
		}
		freeMemory(page_dummy);
		return 0;
		// if is rid
	} else {
		if (record_length != sizeof(int) * 3) {
			cout << "there is a problem about record's RID length, it != 12" << endl;
		}
		record_offset += sizeof(int);
		memcpy(&(realRid.pageNum), page_dummy + record_offset, sizeof(unsigned));
		record_offset += sizeof(unsigned);
		memcpy(&(realRid.slotNum), page_dummy + record_offset, sizeof(unsigned));
	}

	freeMemory(page_dummy);
	if (record_flag) return 0;
	else return -1;
}

/**
 * given the rid and attribute index,
 * get the attribute
 */
RC RecordBasedFileManager::getRealRecordAttribute(const RID &rid,
		char* page_dummy, int idx, char* data) {
	// get the record slot information
	int record_offset = 0;
	int record_length = 0;
	getSlot(record_offset, record_length, rid.slotNum, page_dummy);
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, page_dummy + record_offset, sizeof(int));
	if (!record_flag) return -1;
	// get the attribute_offset
	int record_offset_dummy = record_offset + sizeof(int) * 2 * (1 + idx);
	int attribute_offset = 0;
	memcpy(&attribute_offset, page_dummy + record_offset_dummy, sizeof(int));
	// get the attribute type
	record_offset_dummy += sizeof(int);
	int type_flag = 0;
	memcpy(&type_flag, page_dummy + record_offset_dummy, sizeof(int));
	// get the attribute length
	int attribute_length = 4;
	if (type_flag == 2) {
		memcpy(&attribute_length, page_dummy + record_offset + attribute_offset, sizeof(int));
	}
	// get the attribute
	if (type_flag == 2) {
		memcpy(data, page_dummy + record_offset + attribute_offset + sizeof(int), attribute_length);
	} else {
		memcpy(data, page_dummy + record_offset + attribute_offset, attribute_length);
	}
	return 0;
}

/**
 * reorganize the page given the page number
 */
RC RecordBasedFileManager::reorganizePage(FileHandle &fileHandle,
		const vector<Attribute> &recordDescriptor, const unsigned pageNumber) {
	if (fileHandle.getFile() == NULL) return -1;
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL) return -1;
	char* new_page = (char*) malloc(PAGE_SIZE);
	if (new_page == NULL) return -1;
	if (fileHandle.readPage(pageNumber, page_dummy)) {
		freeMemory(page_dummy);
		return -1;
	}
	if (compressPage(new_page, page_dummy)) {
		freeMemory(new_page);
		freeMemory(page_dummy);
		return -1;
	}
	if (fileHandle.writePage(pageNumber, new_page)) {
		freeMemory(new_page);
		freeMemory(page_dummy);
		return -1;
	}
	freeMemory(new_page);
	freeMemory(page_dummy);
	return 0;
}

/**
 * compress old page and store it in new page
 */
RC RecordBasedFileManager::compressPage(char* new_page, char* old_page) {
	int new_offset = 0;
	int old_offset = PAGE_SIZE - sizeof(int) * 2;
	int slot_num = 0;
	memcpy(&slot_num, old_page + old_offset, sizeof(int));
	// compress records
	for (int i = 0; i < slot_num; i++) {
		// get record flag
		old_offset -= (sizeof(int) * 3);
		int record_flag = 0;
		memcpy(&record_flag, old_page + old_offset, sizeof(int));
		if (record_flag == 0)
			continue;
		// get record offset
		old_offset += sizeof(int);
		int record_offset = 0;
		memcpy(&record_offset, old_page + old_offset, sizeof(int));
		// get record length
		old_offset += sizeof(int);
		int record_length = 0;
		memcpy(&record_length, old_page + old_offset, sizeof(int));
		// write into new page
		if (new_offset + record_length >= PAGE_SIZE) {
			cout << "there is a problem about compress page!" << endl;
			return -1;
		}
		memcpy(new_page + new_offset, old_page + record_offset, record_length);
		// update the new record offset in old page
		old_offset -= sizeof(int);
		memcpy(old_page + old_offset, &new_offset, sizeof(int));
		// update relevant data
		new_offset += record_length;
		old_offset -= sizeof(int);
	}
	// update the free space offset in old page
	old_offset = PAGE_SIZE - sizeof(int);
	memcpy(old_page + old_offset, &new_offset, sizeof(int));
	// copy slot directory to new page
	int slot_dir_length = slot_num * sizeof(int) * 3 + sizeof(int) * 2;
	int slot_dir_offset = PAGE_SIZE - slot_dir_length;
	if (slot_dir_offset < 0) {
		cout << "there is a problem about compress page!" << endl;
		return -1;
	}
	memcpy(new_page + slot_dir_offset, old_page + slot_dir_offset, slot_dir_length);
	return 0;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor,
		const string &conditionAttribute, const CompOp compOp, const void *value,
		const vector<string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator)
{
	if(fileHandle.getFile() == NULL) return -1;
	rbfm_ScanIterator.setFileHandle(fileHandle);
	rbfm_ScanIterator.setRecordDescriptor(recordDescriptor);
	rbfm_ScanIterator.setCompOp(compOp);
	rbfm_ScanIterator.setValue(value);
	rbfm_ScanIterator.setAttributeNames(attributeNames);
	rbfm_ScanIterator.setPageNumber(fileHandle);
	rbfm_ScanIterator.setConditionAttribute(conditionAttribute);
	return 0;
}

// ================================================= Iterator =================================================


bool operator==(const RID &rid1, const RID &rid2) {
	return rid1.pageNum == rid2.pageNum && rid1.slotNum == rid2.slotNum;
}

bool operator!=(const RID &rid1, const RID &rid2) {
	return !(rid1 == rid2);
}

bool operator<(const RID &rid1, const RID &rid2) {
	if(rid1.pageNum<rid2.pageNum) return true;
	else if (rid1.pageNum>rid2.pageNum) return false;
	else {
		return rid1.slotNum < rid2.slotNum;
	}
}

bool operator<=(const RID &rid1, const RID &rid2) {
	if(rid1.pageNum<rid2.pageNum) return true;
	else if(rid1.pageNum>rid2.pageNum) return false;
	else {
		return rid1.slotNum <= rid2.slotNum;
	}

}

bool operator>(const RID &rid1, const RID &rid2) {
	if(rid1.pageNum>rid2.pageNum) return true;
	else if(rid1.pageNum<rid2.pageNum) return false;
	else {
		return rid1.slotNum > rid2.slotNum;
	}
}

bool operator>=(const RID &rid1, const RID &rid2) {
	if(rid1.pageNum>rid2.pageNum) return true;
	else if(rid1.pageNum<rid2.pageNum) return false;
	else {
		return rid1.slotNum >= rid2.slotNum;
	}
}

RBFM_ScanIterator::RBFM_ScanIterator():_value(NULL) {
	_page_number = 0;
	_page_block_pointer = 0;
	_page_pointer = -1;
	_slot_number = 0;
	_slot_pointer = -1;
	_page_dummy = (char *)malloc(_page_block_size * PAGE_SIZE);
}


RBFM_ScanIterator::~RBFM_ScanIterator() {
	if(_page_dummy!=NULL) free(_page_dummy);
	_page_dummy = NULL;
}

/**
 * get the next record
 * 0 - success
 * -1 - fail, reached the end of the file
 */
RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {

	// check whether the page is empty
	if(_page_number==0) return -1;
	// get the next record
	// check whether reached EOF
	int res = -1;
	if(isEOF()) return RBFM_EOF;
	bool is_qualified = false;
	while(res||!is_qualified) {
		res = getNextRecordHelper(rid, data, is_qualified);
		if(!res&&is_qualified) return 0;
		if(isEOF()) return RBFM_EOF;
	}
	return RBFM_EOF;
}

/**
 * test whether reached the end of the file
 * the file must has at least slot
 * no matter whether it is a tomb
 */
bool RBFM_ScanIterator::isEOF() {
	//if(_page_pointer==-1) return true;
	// _page_pointer == _page_block_size;
	// _page_pointer != _page_block_size;
	int current_page = _page_block_pointer * _page_block_size + _page_pointer % _page_block_size;
	return current_page == _page_number;
}

/**
 * given the current member variable:
 * _page_block_pointer, _page_pointer, _slot_pointer
 * to get current record
 * 0 - success, regardless of whether the record is satisfied
 * -1 - fail
 *    - maybe the record has been read
 *    - maybe the record has been deleted
 */
RC RBFM_ScanIterator::getNextRecordHelper(RID &rid, void *data, bool &is_qualified) {
	// check whether need to read pages from disk
	if(_page_pointer==_page_block_size||_page_pointer==-1) {
		getBlockFromDisk();
		_page_pointer = 0;
	}
	// skip the free space directory page
	if(_page_pointer==0) _page_pointer++;
	// check whether need to get the slot number of new page
	if(_slot_pointer==_slot_number||_slot_pointer==-1) {
		getSlotNumber();
		_slot_pointer = 0;
	}
	// start to read
	if(_slot_pointer<_slot_number) {
		rid.pageNum = _page_block_pointer * _page_block_size + _page_pointer;
		rid.slotNum = _slot_pointer;
		// check whether this slot has been read
		set<RID>::iterator got = _hash_set.find(rid);
		if(got!=_hash_set.end()) {
			updateSlotPointer();
			return -1;
		}
		// check whether this slot is a tomb
		int record_flag = 0;
		int offset = _page_pointer * PAGE_SIZE + PAGE_SIZE - sizeof(int) * 2 - sizeof(int) * 3 * (rid.slotNum + 1);
		memcpy(&record_flag, _page_dummy + offset, sizeof(int));
		if(record_flag==0) {
			updateSlotPointer();
			return -1;
		}
		// start read the slot
		int res = -1;
		RID realRid = { rid.pageNum, rid.slotNum };
		RID temp = { rid.pageNum, rid.slotNum };
		while(res) {
			res = getRealConditionalRecord(temp, realRid, data, is_qualified);
			if(!res) {
				updateSlotPointer();
				return 0;
			}
			temp.pageNum = realRid.pageNum;
			temp.slotNum = realRid.slotNum;
		}
	} else {
		updateSlotPointer();
		cout << "getNextRecordHelper exception" << endl;
		return -1;
	}
	cout << "getNextRecordHelper exception" << endl;
	return RBFM_EOF;
}

/**
 * given the current member variable
 * _page_block_pointer, _page_number
 * to read a new block from block
 */
void RBFM_ScanIterator::getBlockFromDisk() {
	int total_page_pointer = _page_block_pointer * _page_block_size;
	int max_number = _page_block_size;
	if(max_number>_page_number - total_page_pointer) {
		max_number = _page_number - total_page_pointer;
	}
	char* page_copy = (char*) malloc(PAGE_SIZE);
	if(page_copy==NULL) {
		cout << "reading block fail" << endl;
		return;
	}
	for(int i=0;i<max_number;i++) {
		if(_file_handle.readPage(total_page_pointer+i, page_copy)) {
			if(page_copy!=NULL) free(page_copy);
			page_copy = NULL;
			cout << "reading block fail" << endl;
			return;
		}
		memcpy(_page_dummy+i*PAGE_SIZE, page_copy, PAGE_SIZE);
	}
}

/**
 * given the current member variable
 * get the number of slot of new page
 */
void RBFM_ScanIterator::getSlotNumber() {
	int page_offset = _page_pointer * PAGE_SIZE;
	page_offset += PAGE_SIZE - sizeof(int) * 2;
	memcpy(&_slot_number, _page_dummy+page_offset, sizeof(int));
}

/**
 * after read a rid, no matter success or fail
 * update the _slot_pointer, _page_pointer, _page_block_size
 */
void RBFM_ScanIterator::updateSlotPointer() {
	if (++_slot_pointer == _slot_number) {
		if (++_page_pointer == _page_block_size) {
			_page_block_pointer++;
		}
	}
}

/**
 * get given the rid, get the current content
 * rid must not be tomb, as well as hasn't been read
 * 0 - point to record
 * -1 - point to another rid
 */
RC RBFM_ScanIterator::getRealConditionalRecord(RID &rid, RID &realRid, void* data, bool &is_qualified) {

	if (_file_handle.getFile() == NULL) return -1;
	// get the record slot information
	int local_page_pointer = rid.pageNum % _page_block_size;
	int local_page_offset = local_page_pointer * PAGE_SIZE;
	int local_offset = PAGE_SIZE - sizeof(int) * 2 - sizeof(int) * 3 * (rid.slotNum + 1);
	// get the record_offset
	local_offset += sizeof(int);
	int record_offset = 0;
	memcpy(&record_offset, _page_dummy+local_page_offset+local_offset, sizeof(int));
	// get the record_length
	local_offset += sizeof(int);
	int record_length = 0;
	memcpy(&record_length, _page_dummy+local_page_offset+local_offset, sizeof(int));
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, _page_dummy+local_page_offset+record_offset, sizeof(int));
	// if is record
	if (record_flag) {
		// found the real record
		filterRecord(_page_dummy+local_page_offset, record_offset, data, is_qualified);
		return 0;
		// if is RID
	} else {
		if (record_length != sizeof(int) * 3) {
			cout << "getRealConditionalRecord: problem about record's RID length, it != 12" << endl;
		}
		// get rid's page number
		record_offset += sizeof(int);
		memcpy(&(realRid.pageNum), _page_dummy+local_page_offset+record_offset, sizeof(int));
		// get rid's slot number
		record_offset += sizeof(int);
		memcpy(&(realRid.slotNum), _page_dummy+local_page_offset+record_offset, sizeof(int));
		// check whether has been read
		if(realRid.pageNum<=rid.pageNum) {
			is_qualified = false;
			return 0;
		}
		set<RID>::iterator got = _hash_set.find(realRid);
		if(got!=_hash_set.end()) {
			is_qualified = false;
			return 0;
		}
		// whether the rid in current block
		if(realRid.pageNum/_page_block_size == _page_block_pointer) {
			// means the passed rid points to rid
			// return to upper method
			return -1;
		} else {
			// individually read the record
			RC res = -1;
			RID temp = { realRid.pageNum, realRid.slotNum };
			while (res) {
				res = independentGetRequestedAttributes(temp, realRid, data, is_qualified);
				if (!res) return 0;
				temp.pageNum = realRid.pageNum;
				temp.slotNum = realRid.slotNum;
			}
			cout << "getRealConditionalRecord -> exception" << endl;
			return -1;
		}
	}
}

/**
 * pass the page_dummy and record_offset
 * test whether satisfy the condition
 * and get the data by request attributes
 */
void RBFM_ScanIterator::filterRecord(char *page_dummy, int record_offset, void *data, bool &is_qualified) {
	if(resolveCondition(page_dummy, record_offset)) {
		readRequestedAttributes(page_dummy, record_offset, data, is_qualified);
	} else {
		is_qualified = false;
		return;
	}
}

/**
 * resolve the condition
 * test whether record satisfy
 */
bool RBFM_ScanIterator::resolveCondition(char* page_dummy, int record_offset) {
	// check whether there is a condition
	if(_comp_op == NO_OP) return true;
	// get the attribute index
	int idx = 0;
	AttrType attr_type = TypeInt;
	for (; idx < _record_descriptor.size(); idx++) {
		Attribute tempAtt = _record_descriptor[idx];
		if (tempAtt.name.compare(_condition_attribute)==0) {
			attr_type = tempAtt.type;
			break;
		}
	}
	// get the attribute offset
	int local_offset = sizeof(int) * 2 * (1 + idx);
	int attribute_offset = 0;
	memcpy(&attribute_offset, page_dummy+record_offset+local_offset, sizeof(int));
	// get the attribute type
	local_offset += sizeof(int);
	AttrType attr_type_check = TypeInt;
	memcpy(&attr_type_check, page_dummy+record_offset+local_offset, sizeof(int));
	if(attr_type!=attr_type_check) {
		cout << "resolveCondition -> the conditional attribute not match" << endl;
		return false;
	}
	return resolveConditionHelper(page_dummy, record_offset, attribute_offset, attr_type);
}

/**
 * resolveCondition's helper
 */
bool RBFM_ScanIterator::resolveConditionHelper(char* page_dummy, int record_offset, int attribute_offset, AttrType attr_type) {
	// get the attribute length
	int attribute_length = 4;
	if (attr_type == TypeVarChar) {
		memcpy(&attribute_length, page_dummy + record_offset + attribute_offset, sizeof(int));
	}
	float compare_res = 0;
	if (attr_type == TypeInt) {
		int value = 0;
		memcpy(&value, page_dummy + record_offset + attribute_offset, attribute_length);
		int condition = 0;
		memcpy(&condition, _value, attribute_length);
		compare_res = value - condition;
	} else if (attr_type == TypeReal) {
		float value = 0;
		memcpy(&value, page_dummy + record_offset + attribute_offset, attribute_length);
		float condition = 0;
		memcpy(&condition, _value, attribute_length);
		compare_res = value - condition;
	} else {
		char* data = (char *) malloc(attribute_length + 1);
		if (data == NULL) {
			cout << "resolveConditionHelper -> malloc error" << endl;
			return false;
		}
		// get the attribute
		memcpy(data, page_dummy + record_offset + attribute_offset + sizeof(int), attribute_length);
		char tail = '\0';
		memcpy((char*)data + attribute_length, &tail, sizeof(char));
		string value(data);
		int realValueSize = 0;
		memcpy(&realValueSize, _value, sizeof(int));
		char* realValue = (char*) malloc(realValueSize + 1);
		memcpy(realValue, (char*)_value + sizeof(int), realValueSize);
		memcpy(realValue + realValueSize, &tail, sizeof(char));
		string condition(realValue);
		compare_res = (float) value.compare(condition);
		free(data);
		data = 0;
	}
	switch (_comp_op) {
	case EQ_OP :
		return compare_res == 0;
		break;
	case LT_OP :
		return compare_res < 0;
		break;
	case GT_OP :
		return compare_res > 0;
		break;
	case LE_OP :
		return compare_res <= 0;
		break;
	case GE_OP :
		return compare_res >= 0;
		break;
	case NE_OP :
		return compare_res != 0;
		break;
	default:
		return false;
		break;
	}
	cout << "isMatchCondition -> exception" << endl;
	return false;
}

/**
 * given the page_dummy and record_offset
 * read the requested attributes
 */
void RBFM_ScanIterator::readRequestedAttributes(char *page_dummy, int record_offset, void *data, bool &is_qualified) {
	int data_offset = 0;
	for(int i=0;i<_attribute_names.size(); i++) {
		string attr_name = _attribute_names[i];
		int idx = 0;
		AttrType attr_type = TypeInt;
		for(;idx<_record_descriptor.size();idx++) {
			Attribute tempAtt = _record_descriptor[idx];
			if(tempAtt.name.compare(attr_name)==0) {
				attr_type = tempAtt.type;
				break;
			}
		}
		// get the attribute offset
		int local_offset = sizeof(int) * 2 * (1 + idx);
		int attribute_offset = 0;
		memcpy(&attribute_offset, page_dummy+record_offset+local_offset, sizeof(int));
		// get the attribute type
		local_offset += sizeof(int);
		AttrType attr_type_check = TypeInt;
		memcpy(&attr_type_check, page_dummy+record_offset+local_offset, sizeof(int));
		if(attr_type!=attr_type_check) {
			cout << "readRequestedAttributes -> the conditional attribute not match" << endl;
			is_qualified = false;
			return;
		}
		// get the attribute length
		int attribute_length = 4;
		if (attr_type == TypeVarChar) {
			memcpy(&attribute_length, page_dummy+record_offset+attribute_offset, sizeof(int));
			attribute_length += 4;
		}
		memcpy((char *)data + data_offset, page_dummy+record_offset+attribute_offset, attribute_length);
		data_offset += attribute_length;
	}
	is_qualified = true;
}



/**
 * given the rid to read content
 * the rid is out of current block, so need to read new page from block
 * 0 - record
 * -1 - point to another rid
 */
RC RBFM_ScanIterator::independentGetRequestedAttributes(RID &rid, RID &realRid, void* data, bool &is_qualified) {
	if (_file_handle.getFile() == NULL) return -1;
	char* page_dummy = (char*) malloc(PAGE_SIZE);
	if (page_dummy == NULL) return -1;
	if (_file_handle.readPage(rid.pageNum, page_dummy)) {
		if(page_dummy) free(page_dummy);
		page_dummy = NULL;
		return -1;
	}
	// get the record slot information
	int offset = PAGE_SIZE - sizeof(int) * 2 - sizeof(int) * 3 * (rid.slotNum + 1);
	// get the record_offset
	offset += sizeof(int);
	int record_offset = 0;
	memcpy(&record_offset, page_dummy+offset, sizeof(int));
	// get the record_length;
	offset += sizeof(int);
	int record_length = 0;
	memcpy(&record_length, page_dummy+offset, sizeof(int));
	// get the record content
	int record_flag = 0; // 1 - record, 0 - RID
	memcpy(&record_flag, page_dummy + record_offset, sizeof(int));
	// if is record
	if (record_flag) {
		realRid.pageNum = rid.pageNum;
		realRid.slotNum = rid.slotNum;
		filterRecord(page_dummy, record_offset, data, is_qualified);
		if(page_dummy) free(page_dummy);
		page_dummy = NULL;
		return 0;
		// if is RID
	} else {
		if (record_length != sizeof(int) * 3) {
			cout << "independentGetRequestedAttributes -> problem about record's RID length, it != 12" << endl;
		}
		// get rid's page number
		record_offset += sizeof(int);
		memcpy(&(realRid.pageNum), page_dummy + record_offset, sizeof(unsigned));
		// get rid's slot number
		record_offset += sizeof(unsigned);
		memcpy(&(realRid.slotNum), page_dummy + record_offset, sizeof(unsigned));
	}

	if(page_dummy) free(page_dummy);
	page_dummy = NULL;
	if (record_flag) return 0;
	else return -1;
}

/**
 * close
 * similar to destructor
 */
RC RBFM_ScanIterator::close() {
	_page_number = 0;
	_page_block_pointer = 0;
	_page_pointer = -1;
	_slot_number = 0;
	_slot_pointer = -1;
	// if(_page_dummy!=NULL) free(_page_dummy);
	// _page_dummy = NULL;
	return 0;
}



