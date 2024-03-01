/** @file buffer_mgr.c
*  @brief A Buffer Manager File.
*
*  This file provides the implementation for a Buffer
*  Manager, which is capable of managing a fixed number of pages
*  in memory that represent pages from a page file managed by the
*  storage manager (storage_mgr.c)
*  The buffer manager supports the management of multiple buffer
*  pools simultaneously, where each buffer pool is a combination of a
*  page file and the page frames that store pages from that file.
*  Two page replacement strategies, namely FIFO and LRU,
*  have been implemented in this implementation of the buffer manager.
*
*  @author Rushikesh Kadam (A20517258) - rkadam7@hawk.iit.edu
*  @author Haren Amal (A20513547) - hamal@hawk.iit.edu
*  @author Gabriel Baranes (A20521263) - gbaranes@hawk.iit.edu
*/

// system-defined libraries
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

// user-defined libraries
#include "dberror.h"
#include "storage_mgr.h"
#include "buffer_mgr.h"
#include "ds_define.h"

SM_FileHandle *fh;
BufferQueue *bufferQueue;
int numOfReadOps;
int numOfWriteOps;

/**
*
* The BufferQueue structure is used in the implementation of a buffer pool manager that manages the allocation of pages in memory.
* Here we initialize the BufferQueue.
*
*/

void initializeBufferQueue(BM_BufferPool *const bm) //complete refactor haren
{
   PageNode *page[bm->numPages];
   int pageFinal = (bm->numPages);
   pageFinal--;


   int tempPageNumber = pageFinal;



   while(tempPageNumber >= 0){
       page[pageFinal - tempPageNumber] = calloc(1, sizeof(PageNode));
       tempPageNumber--;
   }


   tempPageNumber = 0;
   while(tempPageNumber <= pageFinal){
       page[tempPageNumber]->data = (char *)malloc(PAGE_SIZE);
       page[tempPageNumber]->dirtyFlag = false;
       page[tempPageNumber]->pageNum = -1;
       page[tempPageNumber]->fixCount = 0;
       page[tempPageNumber]->frameNumber = tempPageNumber;
       tempPageNumber++;
   }


   tempPageNumber = 0;
   page[tempPageNumber]->prev = NULL;
   page[tempPageNumber]->next = page[tempPageNumber+1];
   tempPageNumber++;
   while(tempPageNumber <= pageFinal){
       if(tempPageNumber == pageFinal){
           page[tempPageNumber]->next = NULL;
           page[tempPageNumber]->prev = page[tempPageNumber-1];
           tempPageNumber++;  
           continue;
       }
	   int next = tempPageNumber+1;
	   int prev = tempPageNumber-1;
       page[tempPageNumber]->next = page[next];
       page[tempPageNumber]->prev = page[prev];
       tempPageNumber++;
      
   }
   bufferQueue->numOfFilledFrames = 0;
   bufferQueue->frameCount = bm->numPages;
   bufferQueue->front = page[0];
   bufferQueue->rear = page[pageFinal];
}


/**
*
* This function will check whether the BufferQueue is empty
*
*/

bool isQueueEmpty()
{
   return bufferQueue->numOfFilledFrames==0;
}

/**
*
* This function will remove the item from the BufferQueue. Before removing an item it will check whether
* the queue is empty. If the queue is empty it will simply return the error code. Otherwise it will
* remove the desired item.
*/
RC removeBufferItem()
{
	if (isQueueEmpty())
	{
		return RC_EMPTY_QUEUE;
	}

	PageNode *page = bufferQueue->front;

	int tempPageNumber = 0;
	while(tempPageNumber < bufferQueue->numOfFilledFrames){
		if(tempPageNumber == bufferQueue->numOfFilledFrames-1){
        bufferQueue->rear = page;
		break;
    }
	page = page->next;
	tempPageNumber++;
   }

	int rearPage;
	int deletePageIdx = page->pageNum;
	page = bufferQueue->rear;

	tempPageNumber = 0;
	while(tempPageNumber < bufferQueue->frameCount){
		if(page->fixCount){
			rearPage = page->pageNum;
			page = page->prev;
		}
		else if(page->pageNum == bufferQueue->rear->pageNum){
			bufferQueue->rear = bufferQueue->rear->prev;
			bufferQueue->rear->next = (bufferQueue->rear->next) == NULL?bufferQueue->rear->next:NULL;
		}
		else{
			page->prev->next = page->next;
			page->next->prev = page->prev;
		}
		tempPageNumber++;
	}

	if (page->dirtyFlag)
	{
		writeBlock(page->pageNum, fh, page->data);
		numOfWriteOps++;
	}
	--bufferQueue->numOfFilledFrames;
	return rearPage == bufferQueue->rear->pageNum?0:deletePageIdx;
}

/**
*
* This function adds a new buffer item to the BufferQueue.
*
*/
RC addBufferItem(BM_PageHandle *const page, const PageNumber pageNum, BM_BufferPool *const bm)
{
	PageNode *pageNode = (PageNode *)malloc(sizeof(PageNode));
	
	// Check if the buffer pool is full. If it is, remove a page from the buffer pool to make room for the new page.
	int deletePageIdx = (bufferQueue->numOfFilledFrames == bufferQueue->frameCount)?removeBufferItem():-1;

	char *data = (char *)calloc(PAGE_SIZE, sizeof(char));
	pageNode->prev = NULL;
	pageNode->next = NULL;
	pageNode->pageNum = pageNum;
	pageNode->dirtyFlag = false;
	pageNode->frameNumber = 0;
	pageNode->fixCount = 1;
	pageNode->data = data;
		

	numOfReadOps = readBlock(pageNode->pageNum, fh, pageNode->data) == RC_OK?numOfReadOps+1:numOfReadOps;
	page->data = pageNode->data;
	pageNode->next = bufferQueue->front;
	bufferQueue->front->prev = pageNode;
	page->pageNum = pageNode->pageNum = pageNum;
	pageNode->frameNumber = isQueueEmpty()? bufferQueue->front->frameNumber:((deletePageIdx == -1)?(bufferQueue->front->frameNumber + 1):deletePageIdx);
	bufferQueue->front = pageNode;
	bufferQueue->numOfFilledFrames++;
	return RC_OK;
}

/**
*
* This function updates the attributes of buffer pool.
*
*/
void updateBM_BufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy)
{
    bm->pageFile = strdup(pageFileName);
    bm->numPages = numPages;
    bm->strategy = strategy;
	char *buffer = (char *)calloc(numPages, PAGE_SIZE);
    bm->mgmtData = buffer;
}

/**
*
* This function pins a page in the buffer pool.
*
*/
RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    RC res;
    switch (bm->strategy)
    {
        case RS_FIFO:
            res = pinPageWithFIFO(bm, page, pageNum);
            break;
        case RS_LRU:
            res = pinPageWithLRU(bm, page, pageNum);
            break;
        default:
            res = RC_INVALID_STRATEGY;
            break;
    }
    return res;
}

/**
*
* This function initializes the Buffer Pool with its attributes like number of pages, page file name, and replacement strategy.
*
*/
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, const int numPages, ReplacementStrategy strategy, void *stratData)
{
    bufferQueue = malloc(sizeof(BufferQueue));
    fh = malloc(sizeof(SM_FileHandle));

    if (!fh || !bufferQueue) {
        return RC_BUFFER_POOL_INITIALIZE_ERROR;
    }
	//If memory gets allocated, call update function for updating the attributes of the buffer pool.
    updateBM_BufferPool(bm, pageFileName, numPages, strategy);

    RC rc = openPageFile(bm->pageFile, fh);

    if (rc != RC_OK) {
        free(fh);
        free(bufferQueue);
        return rc;
    }
    numOfReadOps = numOfWriteOps = 0;
    initializeBufferQueue(bm);

    return RC_OK;
}


/**
*
* This function will shutdown the buffer pool. It writes any dirty pages back to the disk if they are not being used by any process.
*
*/
RC shutdownBufferPool(BM_BufferPool *const bm)
{
    PageNode *currentPageInfo = bufferQueue->front;
    int numDirtyPagesWritten = 0;
    while (currentPageInfo != NULL) {
        if (currentPageInfo->dirtyFlag && currentPageInfo->fixCount == 0) {
            if (writeBlock(currentPageInfo->pageNum, fh, currentPageInfo->data) != RC_OK)
                return RC_WRITE_FAILED;
            numOfWriteOps++;
            currentPageInfo->dirtyFlag = false;
        }
        currentPageInfo = currentPageInfo->next;
    }
    closePageFile(fh);
    return RC_OK;
}

/**
*
* This function forcefully flushes all the dirty pages to the disk.
*
*/
RC forceFlushPool(BM_BufferPool *const bm)
{
    PageNode *currentPageInfo = bufferQueue->front;
    int idx = 0;
    
     while (idx < bufferQueue->frameCount)
    {
        if (currentPageInfo->dirtyFlag == true)
        {
            if (currentPageInfo->fixCount == 0)
            {
                writeBlock(currentPageInfo->pageNum, fh, currentPageInfo->data);
                numOfWriteOps++;
                currentPageInfo->dirtyFlag = false;
            }
        }
        
        currentPageInfo = currentPageInfo->next;
        idx++;
    }
    return RC_OK;
}

/**
*
* This function unpins the page from the buffer pool
*
*/
RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page)
{
    PageNode *currentPageInfo = bufferQueue->front;
    while(currentPageInfo != NULL && currentPageInfo->pageNum != page->pageNum) {
        currentPageInfo = currentPageInfo->next;
    }
    
    if (!currentPageInfo) {
        return RC_READ_NON_EXISTING_PAGE;
    } else {
        currentPageInfo->fixCount--;
        return RC_OK;
    }
}

/**
*
* This function will write a page from the buffer pool to disk.
*
*/
RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) //check again
{
	
    PageNode *currentPageInfo = bufferQueue->front;
    
    while (currentPageInfo != NULL && currentPageInfo->pageNum != page->pageNum)
    {
        currentPageInfo = currentPageInfo->next;
    }
    
    if (!currentPageInfo)
        return RC_READ_NON_EXISTING_PAGE;

	int writeBlockOut = writeBlock(currentPageInfo->pageNum, fh, currentPageInfo->data);
	numOfWriteOps = (writeBlockOut)?numOfWriteOps:numOfWriteOps+1;
	if(writeBlockOut){
		return RC_WRITE_FAILED;
	}
    
    return RC_OK;
}


/**
*
* This function marks a page in the buffer pool as dirty.
*
*/
RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    PageNode *currentPageInfo = bufferQueue->front;

    while (currentPageInfo) {
        if (currentPageInfo->pageNum == page->pageNum) {
            currentPageInfo->dirtyFlag = true;
            return RC_OK;
        }
        currentPageInfo = currentPageInfo->next;
    }

    return RC_READ_NON_EXISTING_PAGE;
}

/**
*
* This function returns an array of page representing the page currently held in each frame of the buffer pool.
*
*/
PageNumber *getFrameContents(BM_BufferPool *const bm)
{
    PageNumber (*pages)[bm->numPages];
    pages = calloc(bm->numPages, sizeof(int));  
    for (int i = 0; i < bm->numPages; i++)
    {
        PageNode *currentPageInfo = bufferQueue->front;
        while (currentPageInfo)
        {
            if (currentPageInfo->frameNumber == i)
            {
                (*pages)[i] = currentPageInfo->pageNum;
                break;
            }
            currentPageInfo = currentPageInfo->next;
        }
    }
    return *pages;
}

/**
*
* This function returns reference to a boolean array that shows which pages in a buffer pool have been marked as dirty.
*
*/
bool *getDirtyFlags(BM_BufferPool *const bm)
{
    bool (*dirtyFlagArray)[bm->numPages];
    dirtyFlagArray = calloc(bm->numPages, sizeof(bool));

    for (int i = 0; i < bm->numPages; i++)
    {
        PageNode *currentPageInfo = bufferQueue->front;
        while (currentPageInfo != NULL && currentPageInfo->frameNumber != i) {
            currentPageInfo = currentPageInfo->next;
        }

        if (currentPageInfo) {
            (*dirtyFlagArray)[i] = currentPageInfo->dirtyFlag;
        }
    }
    return *dirtyFlagArray;
}

/**
*
* This function returns an array of fixCounts for each page in the buffer pool
*
*/
int *getFixCounts(BM_BufferPool *const bm) {
	
    int (*fixCountsArray)[bm->numPages];
    fixCountsArray = calloc(bm->numPages, sizeof(int));

    for (int i = 0; i < bm->numPages; i++) {
		PageNode *currentPageInfo = bufferQueue->front;
        while (currentPageInfo != NULL && currentPageInfo->frameNumber != i) {
            currentPageInfo = currentPageInfo->next;
        }
        if (currentPageInfo) {
            (*fixCountsArray)[i] = currentPageInfo->fixCount;
        }
    }
    return *fixCountsArray;
}



/**
*
* This function returns the number of pages that have been read from the disk.
*
*/
int getNumReadIO(BM_BufferPool *const bm)
{
	return numOfReadOps?numOfReadOps:0;
}

/**
*
* This function returns the number of pages that have been written to the disk.
*
*/
int getNumWriteIO(BM_BufferPool *const bm)
{
	return numOfWriteOps?numOfWriteOps:0;
}

/**
*
* This function pins a page in the buffer pool using LRU page replacement policy
*
*/
RC pinPageWithLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	bool isPageFound = false;
	PageNode *pageNode = bufferQueue->front;

	int tempPageNumber = 0;
	
	// Loop through all the pages in the buffer pool
	while(tempPageNumber < bm->numPages){

		// If page is found in the buffer pool, set pageFound flag to 1 and break out of the loop, otherwise goto the next page
		if(!isPageFound && pageNode->pageNum == pageNum){
			isPageFound = true;
			break;
		}
		pageNode = pageNode->next;
		tempPageNumber++;
	}

	if (!isPageFound)
	{
    addBufferItem(page, pageNum, bm);
	} 
	else 
	{
    pageNode->fixCount++;
    page->data = pageNode->data;
    page->pageNum = pageNum;

    if (pageNode != bufferQueue->front) {
        if (pageNode == bufferQueue->rear) {
            bufferQueue->rear = pageNode->prev;
        }

        if (pageNode->prev) {
            pageNode->prev->next = pageNode->next;
        }

        if (pageNode->next) {
            pageNode->next->prev = pageNode->prev;
        }

        pageNode->prev = NULL;
        pageNode->next = bufferQueue->front;
        bufferQueue->front->prev = pageNode;
		printf("##Checkpoint##");
        bufferQueue->front = pageNode;
    }
}
	return RC_OK;
}

/**
*
* This function pins a page in the buffer pool using FIFO page replacement policy
*
*/
RC pinPageWithFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
	bool isPageFound = false;
	int numPages = bm->numPages;
	PageNode *backupPageInfo = NULL;
	PageNode *currentPageInfo = bufferQueue->front;

	while(currentPageInfo){
		if(currentPageInfo->pageNum == pageNum){
			isPageFound = true;
			break;
		}
		currentPageInfo = currentPageInfo->next;
	}

	if (isPageFound)
	{
		++currentPageInfo->fixCount;
		page->data = currentPageInfo->data;
		page->pageNum = (page)?pageNum:-1;
		return RC_OK;
	}

	currentPageInfo = bufferQueue->front;
	while (bufferQueue->numOfFilledFrames < bufferQueue->frameCount)
	{
		if (currentPageInfo->pageNum == -1)
		{
			currentPageInfo->pageNum = pageNum;
			page->pageNum = pageNum;
            currentPageInfo->fixCount = 1;
			currentPageInfo->dirtyFlag = false;
			bufferQueue->numOfFilledFrames = bufferQueue->numOfFilledFrames + 1;
			readBlock(currentPageInfo->pageNum, fh, currentPageInfo->data);
            numOfReadOps = readBlock(currentPageInfo->pageNum, fh, currentPageInfo->data) == RC_OK?numOfReadOps+1:numOfReadOps;
			page->data = currentPageInfo->data;
			return RC_OK;
		}
		currentPageInfo = currentPageInfo->next;
	}


	PageNode *newNode = (PageNode *)malloc(sizeof(PageNode));
	newNode->data = NULL;
	newNode->next = NULL;
	newNode->fixCount = 1;
	newNode->dirtyFlag = false;
	newNode->pageNum = pageNum;
	page->pageNum = pageNum;
	newNode->prev = bufferQueue->rear;
	currentPageInfo = bufferQueue->front;
	
	while(currentPageInfo && currentPageInfo->fixCount){
		currentPageInfo = currentPageInfo->next;
	}


	if (!currentPageInfo)
	{
		printf("##Checkpoint: No free buffer##");
		return RC_FULL_BUFFER;
	}

	backupPageInfo = currentPageInfo;
	if (currentPageInfo == bufferQueue->front)
	{
		bufferQueue->front->prev = NULL;
		bufferQueue->front = (bufferQueue)?bufferQueue->front->next:bufferQueue->front;
	}
	else if (currentPageInfo != bufferQueue->rear)
	{
		currentPageInfo->prev->next = currentPageInfo->next;
		currentPageInfo->next->prev = currentPageInfo->prev;
	}
	else
	{
		bufferQueue->rear = currentPageInfo->prev;
		newNode->prev = (bufferQueue)?bufferQueue->rear:newNode->prev;
	}


	if (backupPageInfo->dirtyFlag == true)
	{
		numOfWriteOps = writeBlock(backupPageInfo->pageNum, fh, backupPageInfo->data) == RC_OK?numOfWriteOps+1:numOfWriteOps;
	}
	
	newNode->data = backupPageInfo->data;
	newNode->frameNumber = backupPageInfo->frameNumber;
	bufferQueue->rear->next = newNode;
	bufferQueue->rear = newNode;
	numOfReadOps = readBlock(pageNum, fh, newNode->data) == RC_OK?numOfReadOps+1: numOfReadOps;
	page->data = newNode->data;
	return RC_OK;
}