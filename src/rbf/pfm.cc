#include "pfm.h"
#include <cstdio>

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
	delete _pf_manager;
}

//create file if it does not exist, return 0 if succeed
RC PagedFileManager::createFile(const char *fileName)
{
	FILE* pFile;
	pFile = fopen(fileName,"rb");
	if (pFile)
	{
		return -1;
	}
	else
	{
		fopen(fileName,"wb");
		fclose(pFile);
		return 0;
	}
}

//destory file if it exists, return 0 if succeed
RC PagedFileManager::destroyFile(const char *fileName)
{
	if (!remove(fileName))
		return 0;
	else
		return -1;
}

//open file if it exists and associate it with fileHandle, return 0 if succeed
RC PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle)
{
	FILE* pFile;
	int ret = 0;
	pFile = fopen(fileName,"rb+");
	if (pFile)
	{
		fileHandle.setFile(pFile);
		fseek(pFile, 0, SEEK_END);
		ret = !(ftell(pFile) % PAGE_SIZE)? 0: -1;
		int num = ftell(pFile) / PAGE_SIZE;
		fileHandle.setPage(num);

	}
	else
		ret = -1;
	return ret;
}

//close file and delete associate file pointer in fileHandle, return 0 if succeed
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
	if (!fclose(fileHandle.getFile()))
	{
		fileHandle.setFile(NULL);
		return 0;
	}
	else
	{
		return -1;
	}
}


FileHandle::FileHandle()
{
	pFile_ = NULL;
	numOfPages = 0;
}


FileHandle::~FileHandle()
{
}

void FileHandle::setFile(FILE* const pFile)
{
	pFile_ = pFile;
}

FILE* FileHandle::getFile()
{
	return pFile_;
}

void FileHandle::setPage(int num)
{
	numOfPages = num;
}

//read one page of file into data, return 0 if succeed
RC FileHandle::readPage(PageNum pageNum, void *data)
{
	int ret = 0;
    if (pageNum < numOfPages)
    {
    	//check error
    	ret = !fseek(pFile_, PAGE_SIZE * pageNum, SEEK_SET)? 0: -1;
    	ret = fread(data, 1, PAGE_SIZE, pFile_) == PAGE_SIZE || feof(pFile_)? 0: -1;
    }
    else
    	ret = -1;
    return ret;
}

//write one page of file into data, return 0 if succeed
RC FileHandle::writePage(PageNum pageNum, const void *data)
{
	int ret = 0;
    if (pageNum < numOfPages)
    {
    	//check error
    	ret = !fseek(pFile_, PAGE_SIZE * pageNum, SEEK_SET)? 0: -1;
    	ret = fwrite(data, 1, PAGE_SIZE, pFile_) == PAGE_SIZE? 0: -1;
    }
    else
    	ret = -1;
    return ret;
}

//append one page of file into data, return 0 if succeed
RC FileHandle::appendPage(const void *data)
{
	int ret = 0;
	ret = !fseek(pFile_,PAGE_SIZE * numOfPages, SEEK_SET)? 0: -1;
	ret = fwrite(data, 1, PAGE_SIZE, pFile_) == PAGE_SIZE? 0: -1;
	numOfPages++;
	return ret;
}


unsigned FileHandle::getNumberOfPages()
{
    return numOfPages;
}


