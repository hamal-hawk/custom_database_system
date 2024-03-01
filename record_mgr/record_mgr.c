#include <stdlib.h>
#include <string.h>
#include "record_mgr.h"
#include "tables.h"
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "ds_define.h"


SM_FileHandle fh;
SM_PageHandle ph;
TableManagement tableManagement;
ScanManagement scanManagement;

/**
 * 
 * This function will initialize the the Record Manager
 * 
*/
RC initRecordManager(void *mgmtData)
{
    initStorageManager();
    printf("\n~~~~~~~~~~~~<RECORD MANAGER LOADING>~~~~~~~~~~~~\n");
    memset(tombstonedRIDs, -99, sizeof(tombstonedRIDs));
    return RC_OK;
}

/**
 * 
 * This function will clean up the memory used by the record manager durig the operation.
 * 
*/
RC shutdownRecordManager()
{
    if (ph)
    {
        free(ph);
        ph = NULL;
    }
    printf("\n~~~~~~~~~~~~<RECORD MANAGER SHUTDOWN SUCCESSFULLY>~~~~~~~~~~~~\n");
    return RC_OK;
}

/**
 * 
 * This function will create a table file with the given name and schema, and writes the 
 * table metadata to the first page of the file
 * 
*/
RC createTable(char *name, Schema *schema)
{

    RC status = createPageFile(name);
    if (status != RC_OK)
    {
        return 1;
    }
    else
    {
        ph = (SM_PageHandle)malloc(PAGE_SIZE);

        char tableMetaData[PAGE_SIZE] = {0};
        int pos = snprintf(tableMetaData, PAGE_SIZE, "%s|", name);
        pos += snprintf(tableMetaData + pos, PAGE_SIZE - pos, "%d[", schema->numAttr);

        int i = 0;
        while (i < schema->numAttr)
        {
            char attrInfo[schema->numAttr];
            sprintf(attrInfo, "(%s:%d~%d)", schema->attrNames[i], schema->dataTypes[i], schema->typeLength[i]);
            strcat(tableMetaData, attrInfo);
            i++;
        }
        sprintf(tableMetaData + strlen(tableMetaData), "]%d{", schema->keySize);

        int x = 0;
        while (x < schema->keySize)
        {
            sprintf(tableMetaData + strlen(tableMetaData), "%d", schema->keyAttrs[x]);
            if (x < (schema->keySize - 1))
            {
                strcat(tableMetaData, ":");
            }
            x++;
        }
        strcat(tableMetaData, "}");

        tableManagement.firstFreeLoc.page = 1;
        tableManagement.firstFreeLoc.slot = tableManagement.totalRecords = 0;

        if (status == RC_OK)
        {
            char firstFreeLocStr[20];
            char totalRecordsStr[20];

            sprintf(firstFreeLocStr, "$%d:%d$", tableManagement.firstFreeLoc.page, tableManagement.firstFreeLoc.slot);
            sprintf(totalRecordsStr, "?%d?", tableManagement.totalRecords);

            sprintf(tableMetaData + strlen(tableMetaData), "%s%s", firstFreeLocStr, totalRecordsStr);
        }

        memmove(ph, tableMetaData, PAGE_SIZE);

        status = openPageFile(name, &fh);
        if (status != RC_OK)
        {
            return 1;
        }

        status = writeBlock(0, &fh, ph);
        if (status == RC_OK)
        {
            free(ph);
        }else{
            return 1;
        }
        
        return RC_OK;
    }
}

/**
 * 
 * This function will open a table with a given name, initializes a buffer pool, pins 
 * the first page of the table to read the schema from it, and then unpins it before 
 * returning a success or error code
 * 
*/
RC openTable(RM_TableData *rel, char *name)
{
    RC status;
    BM_BufferPool *buffer_pool_ptr;
    BM_PageHandle *page_handle_ptr;
    if (PAGE_SIZE != NULL)
    {
        ph = (SM_PageHandle)malloc(PAGE_SIZE);
    }

    // Get pointers to page handle and buffer pool
    if (&tableManagement != NULL)
    {
        buffer_pool_ptr = &tableManagement.bufferPool;
        page_handle_ptr = &tableManagement.pageHandle;

        // Initialize buffer pool
        initBufferPool(buffer_pool_ptr, name, 3, RS_FIFO, NULL);
        status = pinPage(buffer_pool_ptr, page_handle_ptr, 0);
    }

    switch (status)
    {
    case RC_OK:
        break;
    default:
        RC_message = "Page failed to pin";
        return RC_PIN_PAGE_FAILED;
    }

    schemaReadFromFile(rel, page_handle_ptr);

    status = unpinPage(buffer_pool_ptr, page_handle_ptr);

    switch (status)
    {
    case RC_OK:
        break;
    default:
        RC_message = "Page failed to unpin";
        return RC_UNPIN_PAGE_FAILED;
    }

    return RC_OK;
}

/**
 * 
 * This fuction will save metadata about the table in a character array and writes it 
 * to the first page of the table file before closing the table.
 * 
*/
RC closeTable(RM_TableData *rel)
{
    char metaData[PAGE_SIZE];
    memset(metaData, '\0', PAGE_SIZE);

    snprintf(metaData, PAGE_SIZE, "%s|", rel->name);

    // Add the number of attributes and record size to the metadata.
    int numAttrs = rel->schema->numAttr;
    int recordSize = tableManagement.recordSize;
    snprintf(metaData + strlen(metaData), PAGE_SIZE - strlen(metaData), "%d[%d", numAttrs, recordSize);

    BM_PageHandle *page_handle = &tableManagement.pageHandle;
    BM_BufferPool *buffer_pool = &tableManagement.bufferPool;

    int i = 0;
    while (i < rel->schema->numAttr)
    {
        char attrData[100];
        int attrDataLength = 0;
        if (rel != NULL && rel->schema != NULL && rel->schema->attrNames != NULL &&
            rel->schema->dataTypes != NULL && rel->schema->typeLength != NULL &&
            i >= 0 && i < rel->schema->numAttr)
        {
            int temp = sizeof(attrData);

            int attrDataLength = 0;
            int maxSize = PAGE_SIZE - 1;

            // concatenate the attribute name, data type and type length to the attribute data string
            attrDataLength += snprintf(attrData + attrDataLength, maxSize - attrDataLength, "(%s:", rel->schema->attrNames[i]);

            // check if the data type is valid before adding it to the string
            if (rel->schema->dataTypes[i] == DT_INT || rel->schema->dataTypes[i] == DT_FLOAT || rel->schema->dataTypes[i] == DT_STRING)
            {
                attrDataLength += snprintf(attrData + attrDataLength, maxSize - attrDataLength, "%d~%d)", rel->schema->dataTypes[i], rel->schema->typeLength[i]);
            }
            else
            {
                return RC_NULL;
            }
        }
        else
        {
            // Handle error case where required data is missing
            // For example, set attr_data_len to 0 or print an error message
        }
        if (attrDataLength >= sizeof(attrData))
        {
            // handle error: attribute data exceeded buffer size
        }
        strncat(metaData, attrData, strlen(attrData));
        i++;
    }

    sprintf(metaData + strlen(metaData), "]%d{", rel->schema->keySize);

    i = 0;
    while (i < rel->schema->keySize)
    {
        int temp = strlen(metaData);
        sprintf(metaData + temp, "%d", rel->schema->keyAttrs[i]);
        if (i < (rel->schema->keySize - 1))
        {
            // Find the length of the existing string in metaData
            size_t metaDataLen = strlen(metaData);

            // Check if there is enough room to append the colon character
            if (metaDataLen + 1 < PAGE_SIZE)
            {
                // Append the colon character at the end of the string
                metaData[metaDataLen] = ':';
                metaData[metaDataLen + 1] = '\0';
            }
            else
            {
                // Handle the buffer overflow case, such as logging an error message or returning an error code
                // ...
            }
        }
        i++;
    }
    if (rel->schema->keySize > 0)
    {
        strcat(metaData, "}");
    }

    if (tableManagement.firstFreeLoc.page >= 0 && tableManagement.firstFreeLoc.slot >= 0)
    {
        char buffer[50];
        sprintf(buffer, "$%d:%d$", tableManagement.firstFreeLoc.page, tableManagement.firstFreeLoc.slot);
        strcat(metaData, buffer);
    }

    if (tableManagement.totalRecords >= 0)
    {
        int len = snprintf(NULL, 0, "?%d?", tableManagement.totalRecords);
        char str[len + 1];
        sprintf(str, "?%d?", tableManagement.totalRecords);
        str[len] = '\0';
        strcat(metaData, str);
    }

    RC status = pinPage(buffer_pool, page_handle, 0);
    if (status != RC_OK)
    {
        RC_message = "Pin page failed  ";
        return RC_PIN_PAGE_FAILED;
    }

    memmove(page_handle->data, metaData, PAGE_SIZE);

    status = markDirty(buffer_pool, page_handle);
    if (status != RC_OK)
    {
        RC_message = "Page 0 Mark Dirty Failed";
        return RC_MARK_DIRTY_FAILED;
    }

    status = unpinPage(buffer_pool, page_handle);
    if (status != RC_OK)
    {
        RC_message = "Unpin Page 0 failed Failed";
        return RC_UNPIN_PAGE_FAILED;
    }

    status = shutdownBufferPool(buffer_pool);
    if (status != RC_OK)
    {
        RC_message = "Shutdown Buffer Pool Failed";

        if (RC_message)
        {
            printf("Error Code: %s", RC_message);
        }
        else
        {
            printf("Unable to assign an error code!!!");
        }
        return RC_BUFFER_SHUTDOWN_FAILED;
    }

    return RC_OK;
}


/***
 *
 *Will delete the table and all data associated with it.This mwthod will call destroyPageFile of storage manager
 * @param name : name of table to br deleted.
 * @return RC_OK
 */
RC deleteTable(char *name)
{
    BM_BufferPool *bm = &tableManagement.bufferPool;

    if (bm)
    {
        if (name == NULL)
        {
            printf("Error Code: Table name can not be null");
            RC_message = "Table name can not be null ";
            return RC_NULL_IP_PARAM;
        }

        RC st = destroyPageFile(name);
        if (st != RC_OK)
        {
            RC_message = "Destroyt Page File Failed";
            printf("Error Code: Destroyt Page File Failed");
            return RC_FILE_DESTROY_FAILED;
        }

        return RC_OK;
    }else{
        return RC_NULL;
    }
}

/**
 * 
 * This function will return the number of touples in a given table from the table management information
 * 
*/
int getNumTuples(RM_TableData *rel)
{
    if (!rel)
    {
        return RC_INVALID_REFERENCE_TO_FILE;
    }
    if (&tableManagement != NULL)
    {
        return tableManagement.totalRecords;
    }
    else
    {
        // handle error or return a default value
    }
}

/**
 * 
 * This function will insert a new record into a table. It will check for available 
 * space in the table, pin the appropriate page, and write the record to the page. 
 * Finally, it will update the bookkeeping information for the table and return an appropriate status code.
 * 
*/
RC insertRecord(RM_TableData *rel, Record *record)
{

    char *pageInfo;
    int blockFactor = 0;
    int firstFreePageNo = 0;
    int firstFreePageSlotNo = 0;

    if (&tableManagement != NULL)
    {
        blockFactor = tableManagement.blockFactor;
        firstFreePageNo = tableManagement.firstFreeLoc.page;
        firstFreePageSlotNo = tableManagement.firstFreeLoc.slot;
    }

    BM_PageHandle *page_handle = &tableManagement.pageHandle;
    BM_BufferPool *buffer_pool = &tableManagement.bufferPool;

    if (firstFreePageSlotNo < 0)
    {
        RC_message = "Invalid Page number ";
        return RC_INVALID_PAGE_SLOT_NUM;
    }

    if (firstFreePageNo < 1)
    {
        RC_message = "Invalid Slot number ";
        return RC_INVALID_PAGE_NUM;
    }

    RC status = pinPage(buffer_pool, page_handle, firstFreePageNo);
    RC_message = status != RC_OK ? "Page failed to pin." : RC_message;
    if (status != RC_OK)
    {
        return RC_PIN_PAGE_FAILED;
    }

    int offset = firstFreePageSlotNo * tableManagement.recordSize;
    pageInfo = page_handle->data;

    record->data[tableManagement.recordSize - 1] = '$';
    memcpy(pageInfo + offset, record->data, tableManagement.recordSize);

    status = markDirty(buffer_pool, page_handle);
    RC_message = status != RC_OK ? "Page failed to mark dirty." : RC_message;
    if (status != RC_OK)
    {
        return RC_MARK_DIRTY_FAILED;
    }

    status = unpinPage(buffer_pool, page_handle);
    RC_message = status != RC_OK ? "Page failed to unpin." : RC_message;
    if (status != RC_OK)
    {
        return RC_UNPIN_PAGE_FAILED;
    }
    record->id.page = firstFreePageNo;
    record->id.slot = firstFreePageSlotNo;

    tableManagement.totalRecords += 1;

    RC bf = (blockFactor - 1);
    if (bf < 0)
    {
        return RC_NULL;
    }
    else if (firstFreePageSlotNo == bf)
    {
        tableManagement.firstFreeLoc.page = firstFreePageNo + 1;
        tableManagement.firstFreeLoc.slot = 0;
    }
    else if (firstFreePageSlotNo < bf)
    {
        tableManagement.firstFreeLoc.slot = firstFreePageSlotNo + 1;
    }
    else
    {
        return RC_NULL;
    }

    return RC_OK;
}

/**
 * 
 * This function will delete a record from a table by finding the appropriate page 
 * and slot number, resetting the record data to null, updating the bookkeeping 
 * information for the table, and returning an appropriate status code
 * 
*/
RC deleteRecord(RM_TableData *rel, RID id)
{
    int pageNum, slotNum, recordSize, blockfactor;
    BM_PageHandle *pageHandle, *bufferPool;

    if (id.page >= 0 && id.slot >= 0 && tableManagement.recordSize > 0 && tableManagement.blockFactor > 0)
    {
        pageNum = id.page;
        slotNum = id.slot;
        recordSize = tableManagement.recordSize;
        blockfactor = tableManagement.blockFactor;
        pageHandle = &tableManagement.pageHandle;
        bufferPool = &tableManagement.bufferPool;
    }
    else
    {
        // handle the case where input parameters are invalid
        // for example: return an error code or print an error message
    }

    RC rc = pinPage(bufferPool, pageHandle, pageNum);
    RC_message = rc != RC_OK ? "Page failed to pin." : RC_message;
    if (rc != RC_OK)
    {
        return RC_PIN_PAGE_FAILED;
    }

    int record_offset = slotNum * recordSize;

    memset(pageHandle->data + record_offset, '\0', recordSize);

    tableManagement.totalRecords = tableManagement.totalRecords - 1;

    rc = markDirty(bufferPool, pageHandle);
    RC_message = rc != RC_OK ? "Page failed to mark dirty." : RC_message;
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = unpinPage(bufferPool, pageHandle);
    RC_message = rc != RC_OK ? "Page failed to unpin." : RC_message;
    if (rc != RC_OK)
    {
        return rc;
    }

    return RC_OK;
}

/**
 * 
 * This function will be for updating a record in a table. It will first retrieve 
 * the necessary information about the record and pin the appropriate page. 
 * Then, it will update the record at the given slot with the new data and mark 
 * the page as dirty. Finally, it will unpin the page and return an appropriate status code.
 * 
*/
RC updateRecord(RM_TableData *rel, Record *record)
{
    int recordSize = tableManagement.recordSize;
    int blockFactor = tableManagement.blockFactor;
    int pageNumber = record->id.page;
    int slotNumber = record->id.slot;

    BM_PageHandle *page_handle = &tableManagement.pageHandle;
    BM_BufferPool *buffer_pool = &tableManagement.bufferPool;

    RC rc = pinPage(buffer_pool, page_handle, pageNumber);
    RC_message = rc != RC_OK?"Page failed to pin.":RC_message;
    if (rc != RC_OK)
    {
        return rc;
    }
    int recordOffet = slotNumber * recordSize;

    memcpy(page_handle->data + recordOffet, record->data, recordSize - 1); 

    rc = markDirty(buffer_pool, page_handle);
    RC_message = rc != RC_OK?"Page failed to mark dirty.":RC_message;
    if (rc != RC_OK)
    {
        return rc;
    }

    rc = unpinPage(buffer_pool, page_handle);
    RC_message = rc != RC_OK?"Page failed to unpin.":RC_message;
    if (rc != RC_OK)
    {
        return rc;
    }

    return RC_OK;
}

/**
 * 
 * This fucntion will retrieve a record from a table based on its RID. 
 * It will first pin the appropriate page, then copy the record's data 
 * into the Record struct. It will also set the record's ID to the given RID. 
 * Finally, it will unpin the page and return an appropriate status code.
 * 
*/
RC getRecord(RM_TableData *rel, RID id, Record *record)
{
    BM_PageHandle *page = &tableManagement.pageHandle;
    BM_BufferPool *bm = &tableManagement.bufferPool;

    if (pinPage(bm, page, id.page) == RC_OK)
    {
        int recordOffset = id.slot * tableManagement.recordSize;                     // this will give starting point of record. remember last value in record is '$' replce it wil
        memcpy(record->data, page->data + recordOffset, tableManagement.recordSize); // case of error check boundry condition also check for reccord->data size
        record->data[tableManagement.recordSize - 1] = '\0';
        record->id.page = id.page;
        record->id.slot = id.slot;
    }
    else
    {
        return RC_PIN_PAGE_FAILED;
    }

    return unpinPage(bm, page) == RC_OK ? RC_OK : RC_UNPIN_PAGE_FAILED;
}

/**
 * 
 * This function will start a scan operation on a table based on a given condition. 
 * It will first initialize the scan handle with the appropriate information, 
 * including the table data, the condition, and the initial record ID. It will 
 * then return an appropriate status code.
 * 
*/
RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *condition)
{
    BM_BufferPool *bm = &tableManagement.bufferPool;
    if(!bm){
        RC_NULL;
    }
    scan->rel = rel;
    scanManagement.condition = condition;
    scanManagement.recordID.page = 1;
    scanManagement.recordID.slot = scanManagement.count = scanManagement.recordID.page - 1;
    scan->mgmtData = &scanManagement;
    return RC_OK;
}

/**
 * 
 * This function will retrieve the next record that satisfies the given condition of an 
 * ongoing scan, starting from the current record's page and slot till the last record is encountered
 * 
*/
RC next(RM_ScanHandle *scan, Record *record)
{
    if (tableManagement.totalRecords < 1)
    {
        return RC_RM_NO_MORE_TUPLES;
    }

    if(scanManagement.count == tableManagement.totalRecords){
        return RC_RM_NO_MORE_TUPLES;
    }

    BM_PageHandle *page = &tableManagement.pageHandle;
    BM_BufferPool *bm = &tableManagement.bufferPool;
    int curPageScan, curSlotScan;
    curSlotScan = curPageScan = 0;
    curPageScan = scanManagement.recordID.page;
    curSlotScan = scanManagement.recordID.slot;
    Value *queryExpResult;
    scanManagement.count = scanManagement.count + 1;
    queryExpResult = (Value *)calloc(sizeof(Value), 1);
    int curTotalRecScan;
    for (curTotalRecScan = scanManagement.count; curTotalRecScan < tableManagement.totalRecords; curTotalRecScan++)
    {
        scanManagement.recordID.page = curPageScan?curPageScan:0;
        scanManagement.recordID.slot = curSlotScan?curSlotScan:0;
        RC_message = getRecord(scan->rel, scanManagement.recordID, record) == RC_OK?RC_message: "Reading the record was unsuccessful";

        evalExpr(record, (scan->rel)->schema, scanManagement.condition, &queryExpResult);
        if (scanManagement.condition && queryExpResult->v.boolV == true)
        {
            record->id.page = curPageScan;
            record->id.slot = curSlotScan;
            if (curSlotScan == tableManagement.blockFactor - 1)
            {
                curPageScan++;
                curSlotScan = 0;
            }
            else
            {
                curSlotScan++;
            }
            scanManagement.recordID.page = curPageScan;
            scanManagement.recordID.slot = curSlotScan;
            return RC_OK;
        }
        else
        {
            queryExpResult->v.boolV == true;
        }

        curSlotScan = (curSlotScan == tableManagement.blockFactor - 1) ? 0 : curSlotScan + 1;
        curPageScan = (curSlotScan == tableManagement.blockFactor - 1) ? curPageScan + 1 : curPageScan;
    }

    queryExpResult->v.boolV == true;
    scanManagement.recordID.page = 1;                                            // records starts from page 1
    scanManagement.recordID.slot = scanManagement.count = scanManagement.recordID.page - 1; // slot starts from 0
    return RC_RM_NO_MORE_TUPLES;
}


/**
 * 
 * This function will reset the scan management variables to their initial values, 
 * effectively ending the ongoing scan and allowing for a new scan to begin.
 * 
*/
RC closeScan(RM_ScanHandle *scan)
{
    scanManagement.count = 0;
    scanManagement.recordID.page = 1; // records starts from page 1
    scanManagement.recordID.slot = 0; // slot starts from 0
    return RC_OK;
}


/**
 * 
 * This funtion will will return the size of a record based on the provided schema. 
 * It will iterate through each attribute in the schema, adding the size of its data 
 * type to the total record size. If the schema is not initialized, the function will 
 * return an error message.
 * 
*/
int getRecordSize(Schema *schema)
{
    if (schema)
    {
        int recordSize = 0, i = 0;

        while (i < schema->numAttr)
        {
            if (schema->dataTypes[i] == DT_INT)
            {
                recordSize = recordSize + sizeof(int);
            }
            else if (schema->dataTypes[i] == DT_STRING)
            {
                recordSize = recordSize + (sizeof(char) * schema->typeLength[i]);
            }
            else if (schema->dataTypes[i] == DT_FLOAT)
            {
                recordSize = recordSize + (sizeof(char) * schema->typeLength[i]);
            }
            else
            {
                recordSize = recordSize + sizeof(bool);
            }
            i++;
        }
        return recordSize;
    }
    else
    {
        RC_message = "Please initialize the schema before proceeding.";
        return RC_SCHEMA_NOT_INIT;
    }
}

/**
 * 
 * This function will dynamically create a schema structure and set its attributes 
 * based on the parameters passed to the function. If the allocation of memory for the 
 * schema fails, an error message will be returned
 * 
*/
Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{

    if(numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL){
        return RC_NULL;
    }
    Schema *createdSchema;

    createdSchema = (Schema *)calloc(sizeof(Schema), 1);
    
    if (createdSchema)
    {
        if (numAttr <= 0 || attrNames == NULL || dataTypes == NULL || typeLength == NULL)
        {
            // Handle the invalid input parameters case, such as logging an error message or returning an error code
            return RC_NULL;
        }
        else
        {
            // Allocate memory for the `Schema` struct
            createdSchema = malloc(sizeof(Schema));
            if (createdSchema == NULL)
            {
                // Handle the case where malloc failed to allocate memory, such as logging an error message or returning an error code
            }
            else
            {
                // Set the `Schema` struct's properties with the input parameters
                createdSchema->numAttr = numAttr?numAttr:0;
                createdSchema->keyAttrs = keys?keys:NULL;
                createdSchema->dataTypes = dataTypes?dataTypes:NULL;
                createdSchema->attrNames = attrNames?attrNames:NULL;
                createdSchema->typeLength = typeLength?typeLength:NULL;
                createdSchema->keySize = keySize?keySize:0;
            }
        }
        tableManagement.recordSize = getRecordSize(createdSchema);
        tableManagement.totalRecords = 0;
        return createdSchema;
    }
    else
    {
        RC_message = "There was a failure in dynamically allocating memory for the schema.";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }
}

/**
 * Free the mememory related to schema
 */
RC freeSchema(Schema *schema)
{
    free(schema);
    return RC_OK;
}

/**
 * 
 * This function will dynamically create a new record structure and initialize its data 
 * and id attributes based on the given schema. If the allocation of memory for the record 
 * data fails, an error message will be returned.
 * 
*/
RC createRecord(Record **record, Schema *schema)
{
    Record *newRec;
    newRec = (Record *)calloc(sizeof(Record), 1);
    if (newRec)
    {
        newRec->data = (char *)calloc(tableManagement.recordSize, sizeof(char));
        newRec->id.page = -1; // set to -1 bcz it has not inserted into table/page/slot
        *record = newRec;
        return RC_OK;
    }
    else
    {
        RC_message = "Memory allocation for the record data has failed.";
        return RC_MELLOC_MEM_ALLOC_FAILED;
    }
}

/**
 * 
 * This function frees memory allocated to a record structure and sets its pointer to NULL
 * If the pointer is already NULL, it returns an error message.
 * 
*/
RC freeRecord(Record *record)
{
    if (!record)
    {
        return RC_NULL_IP_PARAM;
    }
    if (record->data)
    {
        free(record->data);
        record->data = NULL;
    }
    free(record);
    record = NULL;
    return RC_OK;
}

/**
 * 
 * This function will retrieve the value of a specific attribute from a record based on its 
 * attribute number and data type. It will create a new value and set it to the retrieved 
 * attribute value, which will be then passed back to the calling function via a double pointer.
 * 
*/
RC getAttr(Record *record, Schema *schema, int attrNum, Value **value)
{
    Value *lastRecord = NULL;   // Initialize to NULL
    float floatAttrib = 0.0; // Initialize to 0.0
    int attribSize = 0;
    int intAttrib = 0;       // Initialize to 0
    char *subString = NULL;
    int offset = getAttributeOffsetInRecord(schema, attrNum);

    if (schema->dataTypes[attrNum] == DT_INT)
    {
        // Declare variables and initialize where appropriate
        attribSize = sizeof(int);
        char *subString = NULL; // Initialize to NULL

        // Check if the record pointer is NULL before proceeding
        if (record == NULL)
        {
            // Handle the NULL record case, such as logging an error message or returning an error code
            // ...
        }
        else
        {
            // Calculate the offset for the attribute in the record
            int offset = getAttributeOffsetInRecord(schema, attrNum);

            // Check if the attribute offset is valid before proceeding
            if (offset < 0 || offset + attribSize > PAGE_SIZE)
            {
                // Handle the invalid offset case, such as logging an error message or returning an error code
                // ...
                return RC_NULL;
            }
            else
            {
                // Allocate memory for the substring and copy the attribute value from the record
                subString = (char *)malloc(attribSize + 1);
                memset(subString, '\0', attribSize + 1);
                memcpy(subString, record->data + offset, attribSize);

                // Convert the substring to an integer attribute value
                intAttrib = atoi(subString);

                // Create a new integer value with the attribute value and set it as the output value
                MAKE_VALUE(*value, DT_INT, intAttrib);
            }

            // Free the substring memory if it was allocated
            if (subString != NULL)
            {
                free(subString);
            }
        }
    }
    else if (schema->dataTypes[attrNum] == DT_STRING)
    {
        // Declare variables and initialize where appropriate
        attribSize = sizeof(char) * schema->typeLength[attrNum];
        char *subString = NULL; // Initialize to NULL

        // Check if the record pointer is NULL before proceeding
        if (record == NULL)
        {
            // Handle the NULL record case, such as logging an error message or returning an error code
            // ...
            return RC_NULL;
        }
        else
        {
            // Calculate the offset for the attribute in the record
            int offset = getAttributeOffsetInRecord(schema, attrNum);

            // Check if the attribute offset is valid before proceeding
            if (offset < 0 || offset + attribSize > PAGE_SIZE)
            {
                // Handle the invalid offset case, such as logging an error message or returning an error code
                // ...
                return RC_NULL;
            }
            else
            {
                // Allocate memory for the substring and copy the attribute value from the record
                subString = (char *)malloc(attribSize + 1);
                memset(subString, '\0', attribSize + 1);
                memcpy(subString, record->data + offset, attribSize);

                // Create a new string value with the substring value and set it as the output value
                MAKE_STRING_VALUE(*value, subString);
            }

            // Free the substring memory if it was allocated
            if (subString != NULL)
            {
                free(subString);
            }
        }
    }
    else if (schema->dataTypes[attrNum] == DT_FLOAT)
    {
        // Declare variables and initialize where appropriate
        attribSize = sizeof(float);
        char *subString = NULL; // Initialize to NULL

        // Check if the record pointer is NULL before proceeding
        if (record == NULL)
        {
            // Handle the NULL record case, such as logging an error message or returning an error code
            // ...
            return RC_NULL;
        }
        else
        {
            // Calculate the offset for the attribute in the record
            int offset = getAttributeOffsetInRecord(schema, attrNum);

            // Check if the attribute offset is valid before proceeding
            if (offset < 0 || offset + attribSize > PAGE_SIZE)
            {
                // Handle the invalid offset case, such as logging an error message or returning an error code
                // ...
                return RC_NULL;
            }
            else
            {
                // Allocate memory for the substring and copy the attribute value from the record
                subString = (char *)malloc(attribSize + 1);
                memset(subString, '\0', attribSize + 1);
                memcpy(subString, record->data + offset, attribSize);

                // Convert the substring to a float value and create a new float value with it
                float floatAttrib = atof(subString);
                MAKE_VALUE(*value, DT_FLOAT, floatAttrib);
            }

            // Free the substring memory if it was allocated
            if (subString != NULL)
            {
                free(subString);
            }
        }
    }
    else
    {
        // Declare variables and initialize where appropriate
        attribSize = sizeof(bool);
        char *subString = NULL; // Initialize to NULL

        // Check if the record pointer is NULL before proceeding
        if (record == NULL)
        {
            // Handle the NULL record case,
            // possibly by setting an error flag and returning
            return RC_NULL;
        }
        else
        {
            // Allocate memory for subString
            subString = malloc(attribSize + 1); // one extra byte to store '\0' char

            // Check if the memory allocation was successful before proceeding
            if (subString == NULL)
            {
                // Handle the allocation error case,
                // possibly by setting an error flag and returning
                return RC_NULL;
            }
            else
            {
                // Copy data from record to subString
                memcpy(subString, record->data + offset, attribSize);
                subString[attribSize] = '\0'; // set last byte to '\0'

                MAKE_VALUE(*value, DT_BOOL, atoi(subString));

                // Free the memory allocated for subString
                free(subString);
            }
        }
    }

    return RC_OK;
}
/**
 * 
 * This function will set the value of a specific attribute in a record based on its attribute 
 * number and data type, by formatting the value appropriately and writing it to the correct offset 
 * in the record's data array.
 * 
*/
RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{

    char intStr[sizeof(int) + 1] = {0};
    char intStrTemp[sizeof(int) + 1] = {0};

    int offset = getAttributeOffsetInRecord(schema, attrNum)?getAttributeOffsetInRecord(schema, attrNum):0;
    int remainder, quotient;
    remainder = quotient = 0;

    memset(intStr, '0', sizeof(char) * 4);
    char *hexValue = "0001";
    int number = (int)strtol(hexValue, NULL, 16);

    if (schema->dataTypes[attrNum] == DT_INT)
    {
        strRepInt(3, value->v.intV, intStr);
        sprintf(record->data + offset, "%s", intStr);
    }
    else if (schema->dataTypes[attrNum] == DT_STRING)
    {
        int strLength = schema->typeLength[attrNum];
        sprintf(record->data + offset, "%s", value->v.stringV);
    }
    else if (schema->dataTypes[attrNum] == DT_FLOAT)
    {
        sprintf(record->data + offset, "%f", value->v.floatV);
    }
    else
    {
        strRepInt(1, value->v.boolV, intStr);
        sprintf(record->data + offset, "%s", intStr);
    }

    return RC_OK;
}
/**
 * 
 * This function will Convert an integer value to a string representation and store it in 
 * the given char array at the specified position.
 * 
*/
void strRepInt(int position, int value, char *result)
{
    int remainder;
    int digit = 0;
    int endPosition = position;

    for(remainder = value; remainder > 0 && position >=0; position--){
        digit = remainder % 10;
        remainder = remainder / 10;
        result[position] = result[position] + digit;
    }

    int val = endPosition +1;

    result[val + 1] = '\0';
}



/**
 * 
 * This function reads the name of the schema from the metadata string passed as an argument
 * and returns a dynamically allocated string with the schema name.
 * 
*/
void schemaReadFromFile(RM_TableData *rel, BM_PageHandle *h)
{
    char metadata[PAGE_SIZE];
    strcpy(metadata, h->data);
    char *schema_name = readSchemaName(&metadata)?readSchemaName(&metadata):NULL;
    int totalAttribute = readTotalAttributes(&metadata)?readTotalAttributes(&metadata):0;
    char *atrMetadata = readAttributeMetaData(&metadata)?readAttributeMetaData(&metadata):NULL;
    int totalKeyAtr = readTotalKeyAttribute(&metadata)?readTotalKeyAttribute(&metadata):0;
    char *atrKeydt = readAttributeKeyData(&metadata)?readAttributeKeyData(&metadata):NULL;
    char *freeVacSlot = readFreePageSlotData(&metadata)?readFreePageSlotData(&metadata):NULL;

    char **names = getAttributesName(atrMetadata, totalAttribute)?getAttributesName(atrMetadata, totalAttribute):NULL;
    DataType *dt = getAttributeDataType(atrMetadata, totalAttribute)?getAttributeDataType(atrMetadata, totalAttribute):NULL;
    int *sizes = getAttributeSize(atrMetadata, totalAttribute)?getAttributeSize(atrMetadata, totalAttribute):NULL;
    int *keys = extractKeyDataType(atrKeydt, totalKeyAtr)?extractKeyDataType(atrKeydt, totalKeyAtr):NULL;
    int *pageSolt = extractFirstFreePageSlot(freeVacSlot)?extractFirstFreePageSlot(freeVacSlot):NULL;
    int totaltuples = extractTotalRecordsTab(&metadata)?extractTotalRecordsTab(&metadata):0;

    int VAL = 20;
    char **cpNames = (char **)malloc(sizeof(char *) * totalAttribute);
    if(cpNames)
        printf("initialization successful for cpNames");

    DataType *cpDt = (DataType *)malloc(sizeof(DataType) * totalAttribute);
     if(cpDt)
        printf("initialization successful for cpDt");

    int *cpSizes = (int *)malloc(sizeof(int) * totalAttribute);
     if(cpSizes)
        printf("initialization successful for cpSizes");

    int *cpKeys = (int *)malloc(sizeof(int) * totalKeyAtr);
     if(cpKeys)
        printf("initialization successful for cpKeys");

    char *cpSchemaName = (char *)malloc(sizeof(char) * VAL);
     if(cpSchemaName)
        printf("initialization successful for cpSchemaName");
        
    memset(cpSchemaName, '\0', sizeof(char) * VAL);
     if(cpSchemaName)
        printf("initialization successful for cpSchemaName");

    int i = 0;
    while (i < totalAttribute)
    {
        cpNames[i] = (char *)malloc(sizeof(char) * 10);
        strcpy(cpNames[i], names[i]);
        i++;
    }

    memcpy(cpDt, dt, sizeof(DataType) * totalAttribute);

    printf("Memory Copy operation Successfull");
   
    memcpy(cpSizes, sizes, sizeof(int) * totalAttribute);

    printf("Memory Copy operation Successfull");

    memcpy(cpKeys, keys, sizeof(int) * totalKeyAtr);

    printf("Memory Copy operation Successfull");

    memcpy(cpSchemaName, schema_name, strlen(schema_name));
    
    printf("Memory Copy operation Successfull");

    free(names);
    printf("Free operation completed for names");

    free(dt);
    printf("Free operation completed for dt");

    free(sizes);
    printf("Free operation completed for sizes");

    free(keys);
    printf("Free operation completed for keys");

    free(schema_name);
    printf("Free operation completed for schema_name");

    Schema *schema = createSchema(totalAttribute, cpNames, cpDt, cpSizes, totalKeyAtr, cpKeys);

    if (schema)
    {
        printf("Schema successfully initialized...");
    }

    printf("Assigning the attributes to the schema...");
    rel->schema = schema;
    rel->name = cpSchemaName;

    if (&tableManagement)
    {
        if(rel != NULL)
            tableManagement.tableData = rel;

        if(getRecordSize(rel->schema)){
            int n = getRecordSize(rel->schema) + 1;
             tableManagement.recordSize = n;
        }

        RC n = (PAGE_SIZE / tableManagement.recordSize);
        tableManagement.blockFactor = n;

        tableManagement.firstFreeLoc.page = pageSolt[0];
        tableManagement.firstFreeLoc.slot = pageSolt[1];

        if(totaltuples != NULL)
            tableManagement.totalRecords = totaltuples;
    }
}

/**
 * 
 * This function reads the name of the schema from the metadata string passed as an argument
 * and returns a dynamically allocated string with the schema name.
 * 
*/

/**
 * 
 * This function updates all the records in a table based on the criteria specified in the scan handle.
 * 
*/
RC updateScan(RM_TableData *rel, Record *record, Record *updaterecord, RM_ScanHandle *scan)
{
    RC item;
    item = NULL;

    for (item = next(scan, record); item == RC_OK; item = next(scan, record))
    {
        if (updateRecord)
        {
            updaterecord->id.slot = record->id.slot?record->id.slot:0;
            updaterecord->id.page = record->id.page?record->id.page:0;
        }
        if(rel){
            updateRecord(rel, updaterecord);
        }
    }
    return RC_OK;
}

/**
 * 
 * This function reads the name of the schema from the metadata string passed as an argument
 * and returns a dynamically allocated string with the schema name.
 * 
*/
char *readSchemaName(char *scmData)
{
    char *tableName = (char *)calloc(sizeof(char), 20);
    int idx;
    if(idx){
        idx = 0;
    }

    while (scmData[idx] != '|')
    {
        tableName[idx] = scmData[idx++];
    }
    tableName[idx] = '\0';
    return tableName;
}

/**
 * 
 * This function Parse the schema metadata string and extract the total number of attributes specified in it.
 * 
*/
int readTotalAttributes(const char *scmData)
{
    const char *beg = strchr(scmData, '|');
    if (!beg)
    {
        return 0;
    }
    beg++;
    const char *end = strchr(beg, '[');
    if (!end)
    {
        return 0;
    }
    char strNumAtr[20];
    strncpy(strNumAtr, beg, end - beg);
    strNumAtr[end - beg] = '\0';

    return (atoi(strNumAtr));
}

/**
 * 
 * This function reads the total number of key attributes from the schema metadata string.
 * 
*/
int readTotalKeyAttribute(char *scmData)
{
    char *strNumAtr = (char *)calloc(sizeof(int), 2);
    int idx = 0, j;
    while (scmData[idx] != ']')
    {
        idx++;
        printf("Incrementing idx - breakpoint");
    }
    idx++;

    for (j = 0; scmData[idx] != '{'; j++)
    {
        strNumAtr[j] = scmData[idx];
        idx++;
    }
    strNumAtr[j] = '\0';
    int out = atoi(strNumAtr);
    return out;
}

char *readAttributeMetaData(char *scmData)
{
    int val = 100;
    char *atrData = (char *)calloc(sizeof(char), val);
    int i, j;
    i = j = 0;
    for (i = 0; scmData[i] != '['; i++)
    {
        // incrementing i value
    }
    i++;

    while (scmData[i] != ']')
    {
        atrData[j] = scmData[i];
        i++;
        j++;
    }
    atrData[j] = '\0';
    return atrData;
}

/**
 * 
 * This function reads attribute key data from the schema metadata.
 * 
*/
char *readAttributeKeyData(char *scmData)
{
    char *attribData = (char *)calloc(sizeof(char), 50);
    int i, j;
    i = j = 0;

    for (i = 0; scmData[i] != '{'; i++)
    {
        // incrementing i value
    }
    ++i;

    while (scmData[i] != '}')
    {
        attribData[j] = scmData[i];
        i++;
        j++;
    }
    // Null-terminate the string in the attribData array.
    attribData[j] = '\0';

    // Return the null-terminated string.
    char *result = attribData;
    return result;
}

/***
 * 
 * This function reads and extracts the data of a free page slot from the schema data.
 * 
*/
char *readFreePageSlotData(char *scmData)
{
    char *atrData = (char *)malloc(sizeof(char) * 50);
    int i, j = 0;
    for (i = 0; scmData[i] != '$'; i++)
    {
        // incrementing i value
    }
    i++;
    while (scmData[i] != '$')
    {
        atrData[j] = scmData[i];
        i++;
        j++;
    }
    atrData[j] = '\0';
    if(!atrData){
        //handle exception
        printf("NULL ARGUMENTS");
    }
    return atrData;
}

/**
 * 
 *  This function returns an array of attribute names extracted from the schema data
 *
*/
char **getAttributesName(char *scmData, int numAttr)
{
    char **attributesName;
    if(!scmData){
        //handle the exception
        printf("NULL ARGUMENTS");
    }
    attributesName = (char **)calloc(sizeof(char), numAttr);
    int i;
    i = 0;
    while (i < numAttr)
    {
        char *extractedName = extractName(getSingleAttributeData(scmData, i));
        attributesName[i] = malloc(sizeof(char) * strlen(extractedName));
        strcpy(attributesName[i++], extractedName);
        free(extractedName);
    }
    return attributesName;
}

int *getAttributeDataType(char *scmData, int numAtr)
{
    int *dt_type = (int *)malloc(sizeof(int) * numAtr);
    int i = 0;
    while (i < numAtr)
    {
        char *atrDt = getSingleAttributeData(scmData, i);
        dt_type[i++] = extractDataType(atrDt);
        free(atrDt);
    }
    return dt_type;
}

/**
 * 
 * This function allocates memory for an integer array of attribute sizes and returns it
 * It extracts the size of each attribute from the schema data using the extractTypeLength function.
 * 
*/
int *getAttributeSize(char *scmData, int numAttr)
{
    int *attribSize = (int *)malloc(sizeof(int) * numAttr);
    int i = 0;
    while (i < numAttr)
    {
        attribSize[i] = extractTypeLength(getSingleAttributeData(scmData, i));
        i++;
    }
    return attribSize;
}

/***
 * 
 * This function returns the metadata of a single attribute in a table schema data.
 * 
*/
char *getSingleAttributeData(char *scmData, int atrNum)
{
    char *atrData = (char *)calloc(sizeof(char), 30);
    int count, i, j;
    count = i = j = 0;
    for (i = 0; count <= atrNum; i++)
    {
        if (scmData[i] == '(')
            count++;
    }

    while (scmData[i] != ')')
    {
        atrData[j] = scmData[i];
        i++;
        j++;
    }
    atrData[j] = '\0';
    return atrData;
}

/**
 * 
 * This function extracts the name of the attribute from the attribute data string.
 * 
*/
char *extractName(char *data)
{
    char *name = malloc(sizeof(char) * 10);
    memset(name, '\0', sizeof(char) * 10);
    int i = 0;
    while (data[i] != ':' && i < 10)
    { // limit the name length to 10 characters
        name[i] = data[i];
        i++;
    }
    name[i] = '\0';
    return name;
}

/**
 * 
 * This function extracts the data type from a given string and returns it as an integer value.
 * 
*/
int extractDataType(char *data)
{
    char *dType = (char *)calloc(sizeof(char), 10);
    int i, j;
    i = j = 0;
    while (data[i] != ':')
    {
        i++;
    }
    i++;
    while (data[i] != '~')
    {
        dType[j] = data[i++];
        j++;
    }
    dType[j] = '\0';
    int out = atoi(dType);
    free(dType);
    return out;
}
/****
 * 
 * This function extracts the length of a data type from a given string.
 * 
*/
int extractTypeLength(char *data)
{
    char *typeLength = (char *)calloc(sizeof(char), 10);
    int i, j;
    i = j = 0;
    while (data[i] != '~')
    {
        i++;
    }
    i++;
    while (data[i] != '\0')
    {
        typeLength[j] = data[i];
        i++;
        j++;
    }

    typeLength[j] = '\0';
    int out = atoi(typeLength);
    free(typeLength);
    return out;
}
/****
 * 
 * 
 * This functon Extracts the array of integers representing the data types of keys of a 
 * record from the provided data string with a given number of keys.
 * 
 * 
*/
int *extractKeyDataType(char *data, int keyNum)
{
    int *values = calloc(keyNum, sizeof(int));
    char *val = calloc(2, sizeof(char));
    int i = 0, j = 0, x = 0;

    while (data[x] != '\0')
    {
        if (data[x] != ':')
        {
            val[i] = data[x];
            i++;
            
        }
        else
        {
            values[j++] = atoi(val);
            val[0] = '\0';
            i = 0;
        }
        x++;
    }

    values[keyNum - 1] = atoi(val);
    return values;
}

/***
 * 
 * This functiokn extracts the first free page and slot numbers from the provided 
 * data string and returns an array of integers containing these values.
 * 
*/
int *extractFirstFreePageSlot(char *data)
{
    int *values = calloc(2, sizeof(int));
    char *val = calloc(2, sizeof(char));
    int i = 0, j = 0, x = 0;

    while (data[x] != '\0')
    {
        if(data[x] != ':'){
            val[i] = data[x];
            i++;
        }

        else
        {
            values[j++] = atoi(val);
            val[0] = '\0';
            i = 0;
        }
        x++;
    }
    values[1] = atoi(val);
    free(val);
    printf("\n Slot %d", values[1]);
    return values;
}
/**
 * 
 * This function will parse the schema metadata string and extract the total number of records 
 * in the table specified in it.
 * 
*/
int extractTotalRecordsTab(const char *scmData)
{
    const char *beg = strchr(scmData, '?');
    if (!beg)
    {
        return 0;
    }
    beg++;
    const char *end = strchr(beg, '?');
    if (!end)
    {
        return 0;
    }
    char atrData[20];
    strncpy(atrData, beg, end - beg);
    atrData[end - beg] = '\0';

    return (atoi(atrData));
}


/**
 * 
 * This function returns the byte offset of a given attribute number in a record based on its schema.
 * 
*/
int getAttributeOffsetInRecord(Schema *schema, int atrnum)
{
    int offset = 0;
    int pos = 0;
    while (pos < atrnum)
    {
        if (schema->dataTypes[pos] == DT_INT)
        {
            offset += sizeof(int);
        }
        else if (schema->dataTypes[pos] == DT_STRING)
        {
            offset += (sizeof(char) * schema->typeLength[pos]);
        }
        else if (schema->dataTypes[pos] == DT_FLOAT)
        {
            offset += sizeof(float);
        }
        else if (schema->dataTypes[pos] == DT_BOOL)
        {
            offset += sizeof(bool);
        }
        pos++;
    }
    return offset;
}


/**
 * 
 * This function prints the contents of a record.
 * 
*/
void printRecord(char *record, int recLen)
{
    int i = 0;
    while (i < recLen)
    {
        printf(record[i]);
        i++;
    }
}

/**
 * 
 * This function prints the details of a table stored in a Table Management Information structure.
 * 
*/
void printTableInfoDetails(TableManagement *tab_info)
{
    printf("Name of the table: [%s] Record size: [%d] Total records in page (blockFactor) [%d] Total Attributes in table [%d] Total Records in table [%d] Next available page and slot [%d:%d]\n",
           tab_info->tableData->name,
           tab_info->recordSize,
           tab_info->blockFactor,
           tab_info->tableData->schema->numAttr,
           tab_info->totalRecords,
           tab_info->firstFreeLoc.page,
           tab_info->firstFreeLoc.slot);
}

/****
 * 
 * This function prints the data stored in a page.
 * 
*/
void printPageData(char *pageData)
{
    printf("\nPage Data: ");
    int i = 0;
    while (i < PAGE_SIZE)
    {
        printf(pageData[i]);
        i++;
    }
    printf("\n Quiting.");
}