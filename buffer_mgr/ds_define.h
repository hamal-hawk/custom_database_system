#include "buffer_mgr.h"
/*
This code defines data structures and two functions (pinPageWithLRU and pinPageWithFIFO) that are used in buffer management. 
The purpose of these functions is to manage the buffer pool, which is a portion of the memory used to store frequently accessed 
data pages in order to improve performance. pinPageWithLRU uses the Least Recently Used algorithm to replace the page that has not been accessed
for the longest time, while pinPageWithFIFO uses the First-In, First-Out algorithm to replace the page that was first added to the buffer pool.
*/
typedef struct PageNode
{
   char *data;
   int pageNum;
   int frameNumber;
   int fixCount;
   bool dirtyFlag;
   struct PageNode *next;
   struct PageNode *prev;
} PageNode;

typedef struct BufferQueue
{
   PageNode *front;
   PageNode *rear;
   int numOfFilledFrames;
   int frameCount;
} BufferQueue;


RC pinPageWithLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);
RC pinPageWithFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);