/** @file storage_mgr.c
*  @brief A Storage Manager File.
*
*  This file provides the implementation for a simple storage
*  Manager, which is capable of reading blocks from a disk file
*  into memory and writing blocks from memory to a disk file.
*  The Storage Manager not only performs the functions of reading
*  and writing pages from a file but also has the capability to
*  create, open, and close files. It is responsible for maintaining
*  various information related to an open file such as the total
*  number of pages, the current page position for reading/writing,
*  the file name, and either a POSIX file descriptor or a FILE pointer.
*
*  @author Rushikesh Kadam (A20517258) - rkadam7@hawk.iit.edu
*  @author Haren Amal (A20513547) - hamal@hawk.iit.edu
*  @author Gabriel Baranes (A20521263) - gbaranes@hawk.iit.edu
*/

// user-defined libraries
#include "storage_mgr.h"
#include "dberror.h"

// system-defined libraries
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>

FILE *file;

// Here we are initializing the Storage manager
void initStorageManager(void)
{
	printf("\n~~~~~~~~~~~~<STORAGE MANAGER LOADING>~~~~~~~~~~~~");
}

/**
*
*  This function creates a page file, that file is opened in write mode "w+",
*  If the file is successfully opened, a block of memory with PAGE_SIZE is
*  allocated and initialized with the null character. The block is then
*  written to the file using fwrite. Finally, the memory is freed and the
*  file is closed. If the file can not be opened, the function returns the
*  error code as RC_FILE_NOT_FOUND.
*
*/
RC createPageFile(char *fName)
{
	file = fopen(fName, "w+");
	if (file)
	{
		char *emptyBlock = malloc(PAGE_SIZE * sizeof(char));
		memset(emptyBlock, '\0', PAGE_SIZE);
		fwrite(emptyBlock, sizeof(char), PAGE_SIZE, file);
		free(emptyBlock);
		printf("\ncreatePageFile() Executed successfully!\n");
		fclose(file);
		return RC_OK;
	}
    printf("\nDesired file can not be accessed due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_NOT_FOUND\n");

	return RC_FILE_NOT_FOUND;
}

/**
*
* This function opens the desired Page File with the name as fName.
*
*/
RC openPageFile(char *fName, SM_FileHandle *fHandle)
{
	file = fopen(fName, "r+");
	if (file)
	{
		fseek(file, 0, SEEK_END); //moving the file pointer to end of the file
		int lastByte = ftell(file); //assigning the current pointer of the file to the lastByte
		int fileLength = lastByte + 1; //calculating the total length of the file
		int nPages = fileLength / PAGE_SIZE;

		fHandle->fileName = fName;
		fHandle->totalNumPages = nPages;
		fHandle->curPagePos = 0;

		rewind(file); // Moving the file pointer back to the beginning of the file
		printf("\nopenPageFile() Executed successfully!\n");
		return RC_OK;
	}
    printf("\nDesired file can not be accesses due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_NOT_FOUND\n");

	return RC_FILE_NOT_FOUND;
}

/**
*
* This function Closes the page file associated with the SM_FileHandle fHandle
*
*/
RC closePageFile(SM_FileHandle *fHandle)
{
	RC fileOpenCloseFlag = fclose(file);
    file = NULL;
	return (fileOpenCloseFlag == 0) ? RC_OK : RC_FAILED_CLOSE;
}

/**
*
* This function distroys the page file associated with the file name fileName
*
*/
RC destroyPageFile(char *fileName)
{
    if(file == NULL){
        return (remove(fileName) == 0) ? RC_OK : RC_FAILED_REMOVAL;
    }
    printf("File can only be destroyed if it is CLOSED");
    return RC_FILE_NOT_CLOSED;
	
}

/**
*
* This function reads the block associated with the SM_FileHandle fHandle
*
*/
RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    if(fHandle == NULL){
        return RC_FILE_NOT_FOUND;
    }

	if (fHandle->totalNumPages < pageNum) //If page number is out of range, it will throw an error
	{
        // printf("\nThere is an Error in reading a Block!!!\n");
        // printf("\nERROR CODE : RC_READ_NON_EXISTING_PAGE\n");  
		return RC_READ_NON_EXISTING_PAGE;
	}
    if(file){
        fseek(file, pageNum * PAGE_SIZE, SEEK_SET);
	    fread(memPage, sizeof(char), PAGE_SIZE, file); //reading the stream from file and to memPage
	    fHandle->curPagePos = pageNum; //updating the current page position to page number
        printf("\nRead operation completed successfully for the desired block!\n");
        return RC_OK;
    }
    return RC_FILE_NOT_OPENED;
}

/**
*
* This function will return the position of current block associated with the fHandle
*
*/
int getBlockPos(SM_FileHandle *fHandle)
{
    printf("\nCurrent position of the block returned successfully!\n");
	return fHandle?(fHandle->curPagePos):RC_FILE_NOT_FOUND;
}

/**
*
* This function reads the first block associated with the fHandle
*
*/
RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	return fHandle?readBlock(0, fHandle, memPage):RC_FILE_NOT_FOUND;
}

/**
*
* This function reads the previous block, position of the previous block can
* not be less than zero. if it is less than zero it will throw an error
*
*/
RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	if (fHandle){
		RC previousBlockPosition;
		previousBlockPosition = fHandle->curPagePos - 1;

		if (previousBlockPosition < 0){
            printf("\nPosition of the previous block can not be accessed due to an Error!!!\n");
            printf("\nERROR CODE : RC_READ_NON_EXISTING_PAGE\n");
			return RC_READ_NON_EXISTING_PAGE;
		}
        printf("\nRead operation completed successfully for the previous block!\n");
		return readBlock(previousBlockPosition, fHandle, memPage);
	}
    printf("\nRead operation can not be completed due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_NOT_FOUND\n");
	return RC_FILE_NOT_FOUND;
}



/**
*
* This function reads the current block associated with the fHandle. If the fHandle is
* NULL it will throw an error.
*
*/
RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (fHandle){
		RC currentBlockPosition = getBlockPos(fHandle);
		readBlock(currentBlockPosition, fHandle, memPage);
        printf("\nRead operation completed successfully for the current block!\n");
		return RC_OK;
	}
    printf("\nRead operation can not be completed due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_HANDLE_NOT_INIT\n");
	return RC_FILE_HANDLE_NOT_INIT;
}

/**
*
* This function reads the next block associated with the fHandle
*
*/
RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (fHandle){
		RC nextBlockPosition = fHandle->curPagePos + 1;
        printf("\nRead operation completed successfully for the previous block!\n");
		return readBlock(nextBlockPosition, fHandle, memPage);
	}
    printf("\nRead operation can not be completed due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_HANDLE_NOT_INIT\n");
	return RC_FILE_HANDLE_NOT_INIT;
}

/**
*
* This function reads the last block associated with the fHandle. It will calculate
* the last position of the block by subtracting 1 from the total number of pages.
*
*/
RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{

	if (fHandle){
		RC endBlockPosition = fHandle->totalNumPages - 1;
        printf("\nRead operation completed successfully for the previous block!\n");
		return readBlock(endBlockPosition, fHandle, memPage);
	}
    printf("\nRead operation can not be completed due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_NOT_FOUND\n");
	return RC_FILE_NOT_FOUND;
}

/**
*
* This function writes stream of data to the 'file'
*
*/
RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	if (pageNum < 0 || pageNum > fHandle->totalNumPages) //If page number is not within a valid range it will throw an error
        {
        printf("\nWriting failed due to an error!!!\n");
        printf("\nERROR CODE : RC_INVALID_PAGE_RANGE\n");
		return RC_INVALID_PAGE_RANGE;
        }

	if (file)
	{
		bool isFailed = fseek(file, (PAGE_SIZE * pageNum), SEEK_SET);
		if (!isFailed)
		{
			fwrite(memPage, sizeof(char), PAGE_SIZE, file); //It will write the stream into 'file' from memePage
			fHandle->curPagePos = pageNum;
			fseek(file, 0, SEEK_END);
			fHandle->totalNumPages = ftell(file) / PAGE_SIZE;
            printf("\nWrite operation completed successfully for desired block!\n");
			return RC_OK;
		}
        printf("\nWriting failed due to an error!!!\n");
        printf("\nERROR CODE : RC_WRITE_FAILED\n");
		return RC_WRITE_FAILED;

	}
    printf("\nRead operation can not be completed due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_NOT_OPENED\n");
	return RC_FILE_NOT_OPENED;
}

/**
*
* This function writes stream of data to the 'file' into the current block
*
*/
RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage)
{
	RC currentBlockPosition = getBlockPos(fHandle); //For writing into current block it will get the current posotion of the block
	return writeBlock(currentBlockPosition, fHandle, memPage); //It will just call writeBlock() with target position as current block position
}

/**
*
* This function appends an empty block after given block location
*
*/
RC appendEmptyBlock(SM_FileHandle *fHandle)
{

	if (file)
	{
		char *newBlock = (char *)calloc(PAGE_SIZE, sizeof(char)); //creating a new block and allocating the memory
		fseek(file, 0, SEEK_END);
		if (fwrite(newBlock, 1, PAGE_SIZE, file) == PAGE_SIZE)
		{
			(*fHandle).totalNumPages = ftell(file) / PAGE_SIZE; //updating the total number of pages
			(*fHandle).curPagePos = fHandle->totalNumPages - 1; //setting the current page position
            free(newBlock);
            printf("\nAppended an empty block successfully!\n");
			return RC_OK;
		}
        printf("\nAn empty block can not be appended due to an Error!!!\n");
        printf("\nERROR CODE : RC_WRITE_FAILED\n");
		return RC_WRITE_FAILED;	
	}
    printf("\nAn empty block can not be appended due to an Error!!!\n");
    printf("\nERROR CODE : RC_FILE_NOT_FOUND\n");
	return RC_FILE_NOT_FOUND; //it will throw an error if file is not accessible
	
}

/**
*
* This function ensures if total number of pages are greater than the required number of pages, else add more pages
*
*/
RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle)
{
	int pageIndex = (*fHandle).totalNumPages; //getting the total number of pages as a current page index
	if (numberOfPages > pageIndex) 
	{
		int morePage = numberOfPages - pageIndex;
		while (morePage > 0) //it will append empty blocks until file reaches its required capacity
		{
			appendEmptyBlock(fHandle);
			morePage--;
		}
		return RC_OK;
	}
    printf("\nOperation unsuccessful!!!\n");
    printf("\nERROR CODE : RC_WRITE_FAILED\n");
	return RC_WRITE_FAILED;
}
