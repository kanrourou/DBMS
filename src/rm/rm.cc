
#include "rm.h"
#include <cassert>

RelationManager* RelationManager::_rm = 0;

RelationManager* RelationManager::instance()
{
	if(!_rm)
		_rm = new RelationManager();

	return _rm;
}

RelationManager::RelationManager()
{
	//initialize rbfm, ix
	_rbfm = RecordBasedFileManager::instance();
	_ix = IndexManager::instance();

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

	//update cache
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
		RBFM_ScanIterator iter1, iter2;
		FileHandle fileHandle;
		vector<string> attributeNames;
		attributeNames.push_back("tableID");
		rc = _rbfm->openFile(catalogTableName, fileHandle);
		rc = _rbfm->scan(fileHandle, catalogDescriptor,
				"", NO_OP, NULL, attributeNames, iter1);
		void* page = malloc(sizeof(int));
		while(iter1.getNextRecord(rid, page) != RBFM_EOF)
		{
			int id = 0;
			memcpy(&id, page, sizeof(int));
			if (id > tableID)
				tableID = id;
		}
		free(page);
		page = 0;
		rc = iter1.close();
		//read all index name into cache
		attributeNames.pop_back();
		attributeNames.push_back("tableName");
		rc = _rbfm->scan(fileHandle, catalogDescriptor,
				"", NO_OP, NULL, attributeNames, iter2);
		void* data = malloc(PAGE_SIZE);
		assert(data != NULL);
		while(iter2.getNextRecord(rid, data) != RBFM_EOF)
		{
			unsigned strLen = 0;
			memcpy(&strLen, data, sizeof(unsigned));
			void* str = malloc(strLen + 1);
			assert(str != NULL);
			memcpy(str, (char*)data + sizeof(unsigned), strLen);
			char tail = '\0';
			memcpy((char*)str + strLen, &tail, sizeof(char));
			//if table name contains "_index_"
			string substr = "_index_";
			char* ptr = strstr((char*)str, substr.c_str());
			//if it contains index, insert it into index cache
			if (ptr != NULL)
			{
				string indexName((char*)str);
				indexes.insert(indexName);

			}
			free(str);
			str = 0;
		}
		free(data);
		data = 0;
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
        vector<Attribute> attrs;
        rc = getAttributes(tableName, attrs);
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

		//delelte indexes if there is any
		
		for (int i = 0; i < (int)attrs.size(); i++) {
			string indexName = tableName + "_index_" + attrs[i].name;
			//delete cache
			if (indexes.find(indexName) != indexes.end())
			{
				indexes.erase(indexName);
				destroyIndex(tableName, attrs[i].name);
			}

		}
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
	//insert into index
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	assert(rc == 0);
	unsigned offset = 0;
	for (int i = 0; i < (int)attrs.size(); i++)
	{
		string indexName = tableName + "_index_" + attrs[i].name;
		switch(attrs[i].type)
		{
		case TypeInt:
		{
			void* keyInt = malloc(sizeof(int));
			assert(keyInt != NULL);
			memcpy(keyInt, (char*)data + offset, sizeof(int));
			offset += sizeof(int);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle1;
				rc = _ix->openFile(indexName, ixFileHandle1);
				assert(rc == 0);
				rc = _ix->insertEntry(ixFileHandle1, attrs[i], keyInt, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle1);
				assert(rc == 0);
			}
			free(keyInt);
			keyInt = 0;
			break;
		}
		case TypeReal:
		{
			void* keyReal = malloc(sizeof(float));
			assert(keyReal != NULL);
			memcpy(keyReal, (char*)data + offset, sizeof(float));
			offset += sizeof(float);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle2;
				rc = _ix->openFile(indexName, ixFileHandle2);
				assert(rc == 0);
				rc = _ix->insertEntry(ixFileHandle2, attrs[i], keyReal, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle2);
				assert(rc == 0);
			}
			free(keyReal);
			keyReal = 0;
			break;
		}
		case TypeVarChar:
		{
			unsigned strLen = 0;
			memcpy(&strLen, data, sizeof(unsigned));
			void* keyVarChar = malloc(strLen);
			assert(keyVarChar != NULL);
			memcpy(keyVarChar, (char*)data + strLen, strLen);
			offset += (sizeof(unsigned) + strLen);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle3;
				rc = _ix->openFile(indexName, ixFileHandle3);
				assert(rc == 0);
				rc = _ix->insertEntry(ixFileHandle3, attrs[i], keyVarChar, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle3);
				assert(rc == 0);
			}
			free(keyVarChar);
			keyVarChar = 0;
		}
		}
	}
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
	//clear index
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	assert(rc == 0);
	for (int i = 0; i < (int)attrs.size(); i++)
	{
		string indexName = tableName + "_index_" + attrs[i].name;
		if (indexes.find(indexName) != indexes.end()) {
			rc = _ix->destroyFile(indexName);
			assert(rc == 0);
			unsigned numOfPages = 4;
			rc = _ix->createFile(indexName, numOfPages);
			assert(rc == 0);
		}
	}
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
	//delete indexes
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	assert(rc == 0);
	void* page = malloc(PAGE_SIZE);
	assert(page != NULL);
	rc = _rbfm->readRecord(fileHandle, attrs, rid, page);
	unsigned offset = 0;
	assert(rc == 0);
	rc = _rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		cout << "deleteTuple: error in recordDelete";
		return rc;
	}
	for (int i = 0; i < (int)attrs.size(); i++)
	{
		string indexName = tableName + "_index_" + attrs[i].name;

		switch(attrs[i].type)
		{
		case TypeInt:
		{
			void* keyInt = malloc(sizeof(int));
			assert(keyInt != NULL);
			memcpy(keyInt, (char*)page + offset, sizeof(int));
			offset += sizeof(int);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle1;
				rc = _ix->openFile(indexName, ixFileHandle1);
				assert(rc == 0);
				rc = _ix->deleteEntry(ixFileHandle1, attrs[i], keyInt, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle1);
				assert(rc == 0);
			}

			free(keyInt);
			keyInt = 0;
			break;
		}
		case TypeReal:
		{
			void* keyReal = malloc(sizeof(float));
			assert(keyReal != NULL);
			memcpy(keyReal, (char*)page + offset, sizeof(float));
			offset += sizeof(float);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle2;
				rc = _ix->openFile(indexName, ixFileHandle2);
				assert(rc == 0);
				rc = _ix->deleteEntry(ixFileHandle2, attrs[i], keyReal, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle2);
				assert(rc == 0);
			}
			free(keyReal);
			keyReal = 0;
			break;
		}
		case TypeVarChar:
		{
			unsigned strLen = 0;
			memcpy(&strLen, page, sizeof(unsigned));
			void* keyVarChar = malloc(strLen);
			assert(keyVarChar != NULL);
			memcpy(keyVarChar, (char*)page + strLen, strLen);
			offset += (sizeof(unsigned) + strLen);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle3;
				rc = _ix->openFile(indexName, ixFileHandle3);
				assert(rc == 0);
				rc = _ix->deleteEntry(ixFileHandle3, attrs[i], keyVarChar, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle3);
				assert(rc == 0);
				free(keyVarChar);
				keyVarChar = 0;
			}
		}
		}
	}
	free(page);
	page = 0;
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
	//delete indexes
	vector<Attribute> attrs;
	rc = getAttributes(tableName, attrs);
	assert(rc == 0);
	void* page = malloc(PAGE_SIZE);
	assert(page != NULL);
	rc = _rbfm->readRecord(fileHandle, attrs, rid, page);
	unsigned offset = 0;
	assert(rc == 0);
	for (int i = 0; i < (int)attrs.size(); i++)
	{
		string indexName = tableName + "_index_" + attrs[i].name;

		switch(attrs[i].type)
		{
		case TypeInt:
		{
			void* keyInt = malloc(sizeof(int));
			assert(keyInt != NULL);
			memcpy(keyInt, (char*)page + offset, sizeof(int));
			offset += sizeof(int);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle1;
				rc = _ix->openFile(indexName, ixFileHandle1);
				assert(rc == 0);
				rc = _ix->deleteEntry(ixFileHandle1, attrs[i], keyInt, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle1);
				assert(rc == 0);
			}
			free(keyInt);
			keyInt = 0;
			break;
		}
		case TypeReal:
		{
			void* keyReal = malloc(sizeof(float));
			assert(keyReal != NULL);
			memcpy(keyReal, (char*)page + offset, sizeof(float));
			offset += sizeof(float);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle2;
				rc = _ix->openFile(indexName, ixFileHandle2);
				assert(rc == 0);
				rc = _ix->deleteEntry(ixFileHandle2, attrs[i], keyReal, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle2);
				assert(rc == 0);
			}
			free(keyReal);
			keyReal = 0;
			break;
		}
		case TypeVarChar:
		{
			unsigned strLen = 0;
			memcpy(&strLen, page, sizeof(unsigned));
			void* keyVarChar = malloc(strLen);
			assert(keyVarChar != NULL);
			memcpy(keyVarChar, (char*)page + strLen, strLen);
			offset += (sizeof(unsigned) + strLen);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle3;
				rc = _ix->openFile(indexName, ixFileHandle3);
				assert(rc == 0);
				rc = _ix->deleteEntry(ixFileHandle3, attrs[i], keyVarChar, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle3);
				assert(rc == 0);
			}
			free(keyVarChar);
			keyVarChar = 0;
		}
		}
	}
	free(page);
	page = 0;
	rc = _rbfm->updateRecord(fileHandle, recordDescriptor, data, rid);
	if (rc) {
		_rbfm->closeFile(fileHandle);
		return rc;
	}
	rc= _rbfm->closeFile(fileHandle);
	//insert into index
	offset = 0;
	for (int i = 0; i < (int)attrs.size(); i++)
	{
		string indexName = tableName + "_index_" + attrs[i].name;
		switch(attrs[i].type)
		{
		case TypeInt:
		{
			void* keyInt = malloc(sizeof(int));
			assert(keyInt != NULL);
			memcpy(keyInt, (char*)data + offset, sizeof(int));
			offset += sizeof(int);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle1;
				rc = _ix->openFile(indexName, ixFileHandle1);
				assert(rc == 0);
				rc = _ix->insertEntry(ixFileHandle1, attrs[i], keyInt, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle1);
				assert(rc == 0);
			}
			free(keyInt);
			keyInt = 0;
			break;
		}
		case TypeReal:
		{
			void* keyReal = malloc(sizeof(float));
			assert(keyReal != NULL);
			memcpy(keyReal, (char*)data + offset, sizeof(float));
			offset += sizeof(float);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle2;
				rc = _ix->openFile(indexName, ixFileHandle2);
				assert(rc == 0);
				rc = _ix->insertEntry(ixFileHandle2, attrs[i], keyReal, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle2);
				assert(rc == 0);
			}
			free(keyReal);
			keyReal = 0;
			break;
		}
		case TypeVarChar:
		{
			unsigned strLen = 0;
			memcpy(&strLen, data, sizeof(unsigned));
			void* keyVarChar = malloc(strLen);
			assert(keyVarChar != NULL);
			memcpy(keyVarChar, (char*)data + strLen, strLen);
			offset += (sizeof(unsigned) + strLen);
			if (indexes.find(indexName) != indexes.end())
			{
				IXFileHandle ixFileHandle3;
				rc = _ix->openFile(indexName, ixFileHandle3);
				assert(rc == 0);
				rc = _ix->insertEntry(ixFileHandle3, attrs[i], keyVarChar, rid);
				assert(rc == 0);
				rc = _ix->closeFile(ixFileHandle3);
				assert(rc == 0);
			}
			free(keyVarChar);
			keyVarChar = 0;
		}
		}
	}
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

/*
 *
 */

RC RelationManager::createIndex(const string &tableName, const string &attributeName)
{
	RC rc;
	unsigned numOfPages = 4;
	string indexName = tableName + "_index_" + attributeName;
	if (indexes.find(indexName) != indexes.end()) {
		cout << "createIndex: duplicate indexes!" << endl;
		return -1;
	}
	vector<Attribute> attrs;
	vector<Attribute> indexAttrs;
	rc = getAttributes(tableName, attrs);
	assert(rc == 0);
	for (int i = 0; i < (int)attrs.size(); i++) {
		if (attrs[i].name.compare(attributeName) == 0) {
			indexAttrs.push_back(attrs[i]);
			break;
		}
	}
	//update index buffer
	indexes.insert(indexName);
	//create catalog info
	rc = createTable(indexName, indexAttrs);
	assert(rc == 0);
	//create index file
	rc = _ix->createFile(indexName, numOfPages);
	assert(rc == 0);
	//traversal the data file and insert into index
	RM_ScanIterator iter;
	FileHandle fileHandle;
	vector<string> indexAttrNames;
	indexAttrNames.push_back(indexAttrs[0].name);
	assert(rc == 0);
	rc = scan(tableName, "", NO_OP, NULL, indexAttrNames, iter);
	assert(rc == 0);
	//open index file
	IXFileHandle ixFileHandle;
	rc = _ix->openFile(indexName, ixFileHandle);
	assert(rc == 0);
	//rid, data
	RID rid;
	void* data = malloc(indexAttrs[0].length);
	assert(data != NULL);
	while (iter.getNextTuple(rid, data) != RM_EOF) {
		rc = _ix->insertEntry(ixFileHandle, indexAttrs[0], data, rid);
		assert(rc == 0);
	}
	iter.close();
	attrs.clear();
	indexAttrs.clear();
	indexAttrNames.clear();
	return rc;
}

RC RelationManager::destroyIndex(const string &tableName, const string &attributeName)
{
	RC rc;
	string indexName = tableName + "_index_" + attributeName;
	//delete catalog
	rc = deleteTable(indexName);
	assert(rc == 0);
	//delete index files
	rc = _ix->destroyFile(indexName);
	assert(rc == 0);
	//delete index in cache
	if (indexes.find(indexName) != indexes.end())
		indexes.erase(indexName);
	return rc;

}

// indexScan returns an iterator to allow the caller to go through qualified entries in index
RC RelationManager::indexScan(const string &tableName,
		const string &attributeName,
		const void *lowKey,
		const void *highKey,
		bool lowKeyInclusive,
		bool highKeyInclusive,
		RM_IndexScanIterator &rm_IndexScanIterator)
{
    // index file
    char* charAry = (char *) malloc(100);
    assert(charAry != NULL);
    unsigned offset = 0;
    memcpy(charAry, tableName.c_str(), tableName.size());
    offset += tableName.size();
    char underScore = '_';
    memcpy(charAry + offset, &underScore, sizeof(char));
    offset++;
    string indexFlag = "index";
    memcpy(charAry + offset, indexFlag.c_str(), indexFlag.size());
    offset += indexFlag.size();
    memcpy(charAry + offset, &underScore, sizeof(char));
    offset++;
    // short attribute name
    unsigned loc = attributeName.find(".", 0);
    string shortAttrName;
    if(loc!=std::string::npos) {
        shortAttrName = attributeName.substr(loc+1, attributeName.size() - loc);
    } else {
        shortAttrName = attributeName;
    }
    memcpy(charAry + offset, shortAttrName.c_str(), shortAttrName.size());
    offset += shortAttrName.size();
    char tail = '\0';
    memcpy(charAry + offset, &tail, sizeof(char));
    offset++;
    string indexTableName = string(charAry);
    free(charAry);
    // ix_file_handle
	IXFileHandle ix_file_handle;
	_ix->openFile(indexTableName, ix_file_handle);
    // attribute
	vector<Attribute> record_descriptor;
	getAttributes(tableName, record_descriptor);
	Attribute attribute;
	for(int i=0;i<record_descriptor.size();i++) {
		attribute = record_descriptor[i];
		if(attribute.name.compare(shortAttrName) == 0) {
			break;
		}
	}
	if(attribute.name.compare(shortAttrName) != 0) {
		cout << "RelationManager::indexScan -> error" << endl;
        return -1;
	}
	_ix->scan(ix_file_handle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_scan_iterator);
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

RM_IndexScanIterator::RM_IndexScanIterator() {}

RM_IndexScanIterator::~RM_IndexScanIterator() {}

RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key)
{
	return ix_scan_iterator.getNextEntry(rid, key);
}

RC RM_IndexScanIterator::close()
{
	return ix_scan_iterator.close();
}
