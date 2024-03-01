#include "buffer_mgr.h"
#include "record_mgr.h"

#define MAX_TOMBSTONED_RIDS 10000

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

typedef struct TableManagement
{
    int recordSize;
    int totalRecords;
    int blockFactor;
    RID firstFreeLoc;
    RM_TableData *tableData;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;
} TableManagement;

typedef struct ScanManagement
{
    RID recordID;
    Expr *condition;
    int count;
    RM_TableData *tableData;
    BM_PageHandle pageHandle;
    BM_BufferPool bufferPool;
} ScanManagement;



RC pinPageWithLRU(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);
RC pinPageWithFIFO(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum);

void schemaReadFromFile(RM_TableData *, BM_PageHandle *);

char *readSchemaName(char *);
int extractTotalRecordsTab(const char *scmData);
int readTotalAttributes(const char *scmData);
char *readAttributeMetaData(char *);
char *readAttributeKeyData(char *);
char **getAttributesName(char *, int);
char *readFreePageSlotData(char *);
char *extractName(char *);
char *getSingleAttributeData(char *, int);
void strRepInt(int position, int value, char *result);
int tombstonedRIDs[MAX_TOMBSTONED_RIDS];
int readTotalKeyAttribute(char *);
int extractDataType(char *);
int *getAttributeDataType(char *, int);
int extractTypeLength(char *data);
int *getAttributeSize(char *scmData, int numAttr);
int *extractKeyDataType(char *data, int keyNum);
int *extractFirstFreePageSlot(char *);
int getAttributeOffsetInRecord(Schema *, int);