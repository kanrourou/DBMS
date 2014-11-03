
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
	if(!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager()
{
	//initialize rbfm
	_rbfm = RecordBasedFileManager::instance();

	//assign table name
	catalogTableName = "catalog";
	catalogTableID = 0;
	catalogTableFlag = unModifiable;
	columnTableName = "column";
	columnTableID = 1;
	columnTableFlag = unModifiable;

	//initialze catalogTableNameDescriptor
	Attribute attr;
	attr.type = TypeInt;
	attr.name = "tableID";
	attr.length = 4;
	catalogDescriptor.push_back(attr);

	attr.type = TypeVarChar;
	attr.name = "tableName";
	attr.length = 50;
	catalogDescriptor.push_back(attr);

	attr.type = TypeVarChar;
	attr.name = "fileName";
	attr.length = 50;
	catalogDescriptor.push_back(attr);

	attr.type = TypeInt;
	attr.name = "systemUserFlag";
	attr.length = 4;
	catalogDescriptor.push_back(attr);




	//initialze columnTableNameDescriptor
	attr.type = TypeInt;
	attr.name = "tableID";
	attr.length = 4;
	columnDescriptor.push_back(attr);

	attr.type = TypeVarChar;
	attr.name = "columnName";
	attr.length = 50;
	columnDescriptor.push_back(attr);

	attr.type = TypeInt;
	attr.name = "columnType";
	attr.length = 4;
	columnDescriptor.push_back(attr);

	attr.type = TypeInt;
	attr.name = "columnLength";
	attr.length = 4;
	columnDescriptor.push_back(attr);

	attr.type = TypeInt;
	attr.name = "columnPosition";
	attr.length = 4;
	columnDescriptor.push_back(attr);




	nameToID.insert(pair<string, int>(catalogTableName, catalogTableID));
	nameToID.insert(pair<string, int>(columnTableName, columnTableID));
	nameToDescriptor.insert(pair<string, vector<Attribute> >
	(catalogTableName, catalogDescriptor));
	nameToDescriptor.insert(pair<string, vector<Attribute> >
	(columnTableName, columnDescriptor));
	//create catalog and column table if necessary
	createSystemTables();

	//set table id


}

RelationManager::~RelationManager()
{
	delete _rm;
}

void RelationManager::prepareColRecord(int tableID, const Attribute &attr,
		int attrIndex, void* record)
{
	int offset = 0;

	memcpy(record, &tableID, sizeof(int));
	offset += sizeof(int);

	int colNameLen = attr.name.length();
	memcpy((char*)record + offset, &colNameLen, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)record + offset, attr.name.c_str(),colNameLen);
	offset += colNameLen;

	int type = attr.type;
	memcpy((char*)record + offset, &type, sizeof(int));
	offset += sizeof(int);

	int colLen = attr.length;
	memcpy((char*)record + offset, &colLen, sizeof(int));
	offset += sizeof(int);

	memcpy((char*)record + offset, &attrIndex, sizeof(int));
	return;


}

void RelationManager::prepareCatRecord(int tableID, const string &tableName,
		const string &fileName, int flag, void* record)
{
	int offset = 0;

	memcpy(record, &tableID, sizeof(int));
	offset += sizeof(int);

	int tableNameLen = tableName.length();
	memcpy((char*)record + offset, &tableNameLen, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)record + offset, tableName.c_str(), tableNameLen);
	offset += tableNameLen;

	int fileNameLen = fileName.length();
	memcpy((char*)record + offset, &fileNameLen, sizeof(int));
	offset += sizeof(int);
	memcpy((char*)record + offset, fileName.c_str(), fileNameLen);
	offset += fileName.length();

	memcpy((char*)record + offset, &flag, sizeof(int));
	return;
}

RC RelationManager::insertColumnTable(const void* data)
{
	FileHandle fileHandle;
	RID rid;
	RC rc = 0;

	rc = _rbfm->openFile(columnTableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->insertRecord(fileHandle, columnDescriptor, data, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::insertCatalogTable(const void* data)
{
	FileHandle fileHandle;
	RID rid;
	RC rc = 0;

	rc = _rbfm->openFile(catalogTableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->insertRecord(fileHandle, catalogDescriptor, data, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::createSystemTables()
{
	RC rc = _rbfm->createFile(catalogTableName);
	_rbfm->createFile(columnTableName);
	if (rc == -1)
	{
		//initialize tableID
		tableID = 0;
		RID rid;
		RBFM_ScanIterator iter;
		FileHandle fileHandle;
		vector<string> attributeNames;
		attributeNames.push_back("tableID");
		rc = _rbfm->openFile(catalogTableName, fileHandle);
		rc = _rbfm->scan(fileHandle, catalogDescriptor,
				"", NO_OP, NULL, attributeNames, iter);
		void* page = malloc(sizeof(int));
		while(iter.getNextRecord(rid, page) != RBFM_EOF)
		{
			int id = 0;
			memcpy(&id, page, sizeof(int));
			if (id > tableID)
				tableID = id;
		}
		free(page);
		page = 0;
		rc = iter.close();
		rc = _rbfm->closeFile(fileHandle);
		tableID++;
	}
	else
	{
		//initialize tableID
		tableID = 2;
		//insert catalog info into catalog table and column table
		int catalogCatRecordSize = 0, catalogColRecordSize = 0;
		size_t vecLen = catalogDescriptor.size();
		for (size_t i = 0; i < vecLen; i++)
		{
			catalogColRecordSize = (sizeof(int) + sizeof(int) +
					catalogDescriptor[i].name.length() + 3 * sizeof(int));
			void* catalogColRecord = malloc(catalogColRecordSize);
			prepareColRecord(catalogTableID, catalogDescriptor[i], i, catalogColRecord);
			rc = insertColumnTable(catalogColRecord);
			free(catalogColRecord);
			catalogColRecord = 0;
		}
		catalogCatRecordSize = (sizeof(int) + 2 *
				(sizeof(int) + catalogTableName.length()) + sizeof(int));
		void* catalogCatRecord = malloc(catalogCatRecordSize);
		prepareCatRecord(catalogTableID, catalogTableName,
				catalogTableName, catalogTableFlag, catalogCatRecord);
		rc = insertCatalogTable(catalogCatRecord);
		free(catalogCatRecord);
		catalogCatRecord = 0;

		//insert column info into catalog table and column table
		int columnCatRecordSize = 0, columnColRecordSize = 0;
		vecLen = columnDescriptor.size();
		for (size_t i = 0; i < vecLen; i++)
		{
			columnColRecordSize = (sizeof(int) + sizeof(int) +
					columnDescriptor[i].name.length() + 3 * sizeof(int));
			void* columnColRecord = malloc(columnColRecordSize);
			prepareColRecord(columnTableID, columnDescriptor[i], i , columnColRecord);
			rc = insertColumnTable(columnColRecord);
			free(columnColRecord);
			columnColRecord = 0;
		}
		columnCatRecordSize = (sizeof(int) + 2 *
				(sizeof(int) + columnTableName.length()) + sizeof(int));
		void* columnCatRecord = malloc(columnCatRecordSize);
		prepareCatRecord(columnTableID, columnTableName,
				columnTableName, columnTableFlag, columnCatRecord);
		rc = insertCatalogTable(columnCatRecord);
		free(columnCatRecord);
		columnCatRecord = 0;
	}
	return rc;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
	//cannot create system table
	if (tableName == catalogTableName || tableName == columnTableName)
		return -1;
	if (tableName.length() > (int)catalogDescriptor[1].length)
	{
		cout << "cannot assign table name more than " << catalogDescriptor[1].length << endl;
		return -1;
	}
	RC rc;
	rc = _rbfm->createFile(tableName.c_str());
	if (rc == -1)//table exists
	{
		cout << "duplicated table" << endl;
		return rc;
	}
	else
	{
		//assign tableID and flag
		int id = tableID++;
		//update cahce
		nameToID.insert(pair<string, int>(tableName, id));
		nameToDescriptor.insert(pair<string, vector<Attribute> >(tableName, attrs));
		int flag = modifiable;
		int catalogRecordSize = 0, columnRecordSize = 0;
		//update catalog table
		catalogRecordSize = (sizeof(int) +
				2 * (sizeof(int) + tableName.length()) + sizeof(int));
		void* catalogRecord = malloc(catalogRecordSize);
		prepareCatRecord(id, tableName, tableName, flag, catalogRecord);
		rc = insertCatalogTable(catalogRecord);
		free(catalogRecord);
		catalogRecord = 0;
		size_t vecLen = attrs.size();
		//update column table
		for (size_t i = 0; i < vecLen; i++)
		{
			columnRecordSize = (sizeof(int) + sizeof(int) +
					attrs[i].name.length() + 3 * sizeof(int));
			void* columnRecord = malloc(columnRecordSize);
			prepareColRecord(id, attrs[i], i , columnRecord);
			rc = insertColumnTable(columnRecord);
			free(columnRecord);
			columnRecord = 0;
		}
	}
	return rc;

}

RC RelationManager::deleteTable(const string &tableName)
{
	if (tableName == catalogTableName || tableName == columnTableName)
	{
		cout << "users can not modify system tables" << endl;
		return -1;
	}
	RC rc = _rbfm->destroyFile(tableName.c_str());
	if (rc == -1)
	{
		cout << "can not find the table file for deletion" << endl;
		return rc;
	}
	else
	{
		//remove it from cache
		if (nameToID.find(tableName) != nameToID.end())
			nameToID.erase(tableName);
		if (nameToDescriptor.find(tableName) != nameToDescriptor.end())
			nameToDescriptor.erase(tableName);
		FileHandle fileHandle;
		vector<string> attributeNames;
		attributeNames.push_back(catalogDescriptor[0].name);
		RBFM_ScanIterator iterCat;
		//inilize value filter
		int valueSize = (sizeof(int) + tableName.length());
		int tableNameLen = tableName.length();
		void* value = malloc(valueSize);
		memcpy(value, &tableNameLen, sizeof(int));
		memcpy((char*)value + sizeof(int), tableName.c_str(), tableNameLen);
		//data for iterator
		void* data = malloc(sizeof(int));
		RID rid;
		int id = -1;
		//delete table info in catalog
		rc = _rbfm->openFile(catalogTableName, fileHandle);
		rc = _rbfm->scan(fileHandle, catalogDescriptor,
				catalogDescriptor[1].name, EQ_OP, value, attributeNames, iterCat);
		if(iterCat.getNextRecord(rid, data) != RBFM_EOF)
		{
			memcpy(&id, data, sizeof(int));
			rc = _rbfm->deleteRecord(fileHandle, catalogDescriptor, rid);
		}
		free(data);
		data = 0;
		free(value);
		value = 0;
		iterCat.close();
		rc = _rbfm->closeFile(fileHandle);
		if (id == -1)
		{
			cout << "inconsisitency in table file and catalog" << endl;
			return -1;
		}
		//initilize value fileter
		value = malloc(sizeof(int));
		data = malloc(sizeof(int));
		memcpy(value, &id, sizeof(int));
		attributeNames.clear();
		attributeNames.push_back(columnDescriptor[0].name);
		RBFM_ScanIterator iterCol;
		//delete table info in column info
		rc = _rbfm->openFile(columnTableName, fileHandle);
		rc = _rbfm->scan(fileHandle, columnDescriptor,
				columnDescriptor[0].name, EQ_OP, value, attributeNames, iterCol);
		while(iterCol.getNextRecord(rid, data) != RBFM_EOF)
		{
			rc = _rbfm->deleteRecord(fileHandle, columnDescriptor, rid);
		}
		free(value);
		value = 0;
		free(data);
		data = 0;
		iterCol.close();
		rc = _rbfm->closeFile(fileHandle);

	}
	return rc;
}

RC RelationManager::getTableID(const string &tableName, int &tableID)
{
	RC rc;
	if (nameToID.find(tableName) != nameToID.end())
	{
		tableID = nameToID[tableName];
		return 0;
	}
	else
	{
		int id = -1;
		//initialize filter
		FileHandle fileHandle;
		vector<string> attributeNames;
		attributeNames.push_back(catalogDescriptor[0].name);
		RBFM_ScanIterator iterCat;
		int valueSize = (sizeof(int) + tableName.length());
		int tableNameLen = tableName.length();
		void* value = malloc(valueSize);
		memcpy(value, &tableNameLen, sizeof(int));
		memcpy((char*)value + sizeof(int), tableName.c_str(), tableNameLen);
		//data for iterator
		void* data = malloc(sizeof(int));
		RID rid;
		//get id from catalog
		rc = _rbfm->openFile(catalogTableName, fileHandle);
		rc = _rbfm->scan(fileHandle, catalogDescriptor,
				catalogDescriptor[1].name, EQ_OP, value, attributeNames, iterCat);
		if(iterCat.getNextRecord(rid, data) != RBFM_EOF)
		{
			memcpy(&id, data, sizeof(int));
		}
		free(data);
		data = 0;
		free(value);
		value = 0;
		iterCat.close();
		rc = _rbfm->closeFile(fileHandle);
		if (id == -1)
		{
			cout << "table does not exist" << endl;
			return -1;
		}
		tableID = id;
		nameToID.insert(pair<string, int>(tableName, id));
		return rc;
	}
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
	RC rc;
	attrs.clear();//assure empty
	//if it is in cache, we fetch it in cache
	if (nameToDescriptor.find(tableName) != nameToDescriptor.end())
	{
		attrs = nameToDescriptor[tableName];
		return 0;
	}
	else
	{
		map<int, Attribute> positionToAttr;
		int id = -1;
		rc = getTableID(tableName, id);
		if (id == -1)
		{
			return -1;
		}
		//get attribute from column table
		//set filter
		FileHandle fileHandle;
		void* value = malloc(sizeof(int));
		memcpy(value, &id, sizeof(int));
		vector<string> attributeNames;
		attributeNames.push_back(columnDescriptor[1].name);
		attributeNames.push_back(columnDescriptor[2].name);
		attributeNames.push_back(columnDescriptor[3].name);
		attributeNames.push_back(columnDescriptor[4].name);
		RBFM_ScanIterator iter;
		RID rid;
		int dataSize = (4 * sizeof(int) + columnDescriptor[1].length);
		void* data = malloc(dataSize);
		rc = _rbfm->openFile(columnTableName, fileHandle);
		rc = _rbfm->scan(fileHandle, columnDescriptor,
				columnDescriptor[0].name, EQ_OP, value, attributeNames, iter);
		while (iter.getNextRecord(rid, data) != RBFM_EOF)
		{
			Attribute attr;
			int offset = 0;
			//get name
			int nameLen = 0;
			memcpy(&nameLen, data, sizeof(int));
			offset += sizeof(int);
			void* name = malloc(nameLen + 1);
			memcpy(name, (char*)data + offset, nameLen);
			offset += nameLen;
			char tail = '\0';
			memcpy((char*)name + nameLen, &tail, sizeof(char));
			string attrName((char*)name);
			attr.name = attrName;
			free(name);
			name = 0;
			//get type
			int type = -1;
			memcpy(&type, (char*)data + offset, sizeof(int));
			offset += sizeof(int);
			attr.type = (AttrType)type;
			//get length
			int length = 0;
			memcpy(&length, (char*)data + offset, sizeof(int));
			offset += sizeof(int);
			attr.length = length;
			//get position
			int position = -1;
			memcpy(&position, (char*)data + offset, sizeof(int));
			//insert into map
			positionToAttr.insert(pair<int, Attribute>(position, attr));

		}
		free(value);
		value = 0;
		free(data);
		data = 0;
		iter.close();
		rc = _rbfm->closeFile(fileHandle);
		size_t mapSize = positionToAttr.size();
		for (size_t i = 0; i < mapSize; i++)
		{
			attrs.push_back(positionToAttr[i]);
		}
		//update cahce
		nameToID.insert(pair<string, int>(tableName, id));
		nameToDescriptor.insert(pair<string, vector<Attribute> >(tableName, attrs));
		return rc;
	}

}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
	if (tableName == catalogTableName || tableName == columnTableName)
	{
		cout << "users can not modify system tables" << endl;
		return -1;
	}
	vector<Attribute> recordDescriptor;
	RC rc = getAttributes(tableName, recordDescriptor);
	if (rc)
	{
		cout << "insert Tuple cannot get attributes" << endl;
		return -1;
	}
	FileHandle fileHandle;
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->insertRecord(fileHandle, recordDescriptor, data, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::deleteTuples(const string &tableName)
{
	if (tableName == catalogTableName || tableName == columnTableName)
	{
		cout << "users can not modify system tables" << endl;
		return -1;
	}
	RC rc;
	FileHandle fileHandle;
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->deleteRecords(fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return -1;
	}
	rc = _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
	if (tableName == catalogTableName || tableName == columnTableName)
	{
		cout << "users can not modify system tables" << endl;
		return -1;
	}
	RC rc;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		cout << "deleteTuple: error in fileOpen";
		return rc;
	}
	rc = _rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		cout << "deleteTuple: error in recordDelete";
		return rc;
	}
	rc = _rbfm->closeFile(fileHandle);
	if (rc) {
		cout << "deleteTuple: error in fileClose";
	}
	return rc;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
	if (tableName == catalogTableName || tableName == columnTableName)
	{
		cout << "users can not modify system tables" << endl;
		return -1;
	}
	RC rc;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc = _rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc= _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
	RC rc;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc = _rbfm->readRecord(fileHandle, recordDescriptor, rid, data);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc = _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
	RC rc;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc = _rbfm->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc= _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::reorganizePage(const string &tableName, const unsigned pageNumber)
{
	RC rc;
	FileHandle fileHandle;
	vector<Attribute> recordDescriptor;
	getAttributes(tableName, recordDescriptor);
	rc = _rbfm->openFile(tableName, fileHandle);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc = _rbfm->reorganizePage(fileHandle, recordDescriptor, pageNumber);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc= _rbfm->closeFile(fileHandle);
	return rc;
}

RC RelationManager::scan(const string &tableName,
		const string &conditionAttribute,
		const CompOp compOp,
		const void *value,
		const vector<string> &attributeNames,
		RM_ScanIterator &rm_ScanIterator)
{
	FileHandle file_handle;
	_rbfm->openFile(tableName, file_handle);
	vector<Attribute> record_descriptor;
	getAttributes(tableName, record_descriptor);
	_rbfm->scan(file_handle, record_descriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_scan_iterator);
	return 0;

}


// Extra credit
RC RelationManager::dropAttribute(const string &tableName, const string &attributeName)
{
	return -1;
}

// Extra credit
RC RelationManager::addAttribute(const string &tableName, const Attribute &attr)
{
	return -1;
}

// Extra credit
RC RelationManager::reorganizeTable(const string &tableName)
{
	return -1;
}

RM_ScanIterator::RM_ScanIterator() {}

RM_ScanIterator::~RM_ScanIterator() {}

RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
	return rbfm_scan_iterator.getNextRecord(rid, data);
}

RC RM_ScanIterator::close() {
	return rbfm_scan_iterator.close();
}
