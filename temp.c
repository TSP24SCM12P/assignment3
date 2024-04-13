#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "record_mgr.h"
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

/* Macros */

#define PAGE_FILE_NAME "DATA.bin"
#define TABLE_NAME_SIZE 16
#define ATTR_NAME_SIZE 16
#define MAX_NUM_ATTR 8
#define MAX_NUM_KEYS 4
#define MAX_NUM_TABLES PAGE_SIZE / (sizeof(ResourceManagerSchema) + sizeof(int) * 2)

#define USE_PAGE_HANDLE_HEADER(errorValue) \
int const error = errorValue; \
RC result; \
BM_PageHandle handle; \
RM_PageHeader *header; 

#define BEGIN_USE_PAGE_HANDLE_HEADER(pageNum) \
result = pinPage(&bufferPool, &handle, pageNum); \
if (result != RC_OK) return error; \
header = getPageHeader(&handle);

#define END_USE_PAGE_HANDLE_HEADER() \
result = unpinPage(&bufferPool, &handle); \
if (result != RC_OK) return error;

/* Additional Definitions */

typedef struct ResourceManagerSchema {
    char name[TABLE_NAME_SIZE];
    int numAttr;
    char attrNames[MAX_NUM_ATTR * ATTR_NAME_SIZE];
    DataType dataTypes[MAX_NUM_ATTR];
    int typeLength[MAX_NUM_ATTR];
    int keySize;
    int keyAttrs[MAX_NUM_KEYS];
    int numTuples;
    int pageNum;
    BM_PageHandle *handle;
} ResourceManagerSchema;

typedef struct RM_SystemCatalog {
    int totalNumPages;
    int freePage;
    int numTables;
    ResourceManagerSchema tables[MAX_NUM_TABLES];
} RM_SystemCatalog;

typedef struct RM_PageHeader {
    int nextPage;
    int prevPage;
    int numSlots;
} RM_PageHeader;

typedef struct RM_ScanData {
    RID id;
    Expr *cond;
} RM_ScanData;

/* Global variables */

BM_BufferPool bufferPool;
BM_PageHandle catalogPageHandle;

/* Declarations */

RM_SystemCatalog* getSystemCatalog();
RC markSystemCatalogDirty();
ResourceManagerSchema *getTableByName(char *name);
RM_PageHeader *getPageHeader(BM_PageHandle* handle);
bool *getSlots(BM_PageHandle* handle);
char *getTupleData(BM_PageHandle* handle);
int getFreePage();
int setFreePage(BM_PageHandle* handle);
int appendToFreeList(int pageNum);
int getAttrSize(Schema *schema, int attrIndex);
int getNextSlotInWalk(ResourceManagerSchema *table, BM_PageHandle **handle, bool** slots, int *slotIndex);
int closeSlotWalk(ResourceManagerSchema *table, BM_PageHandle **handle);

/* Helpers */

// helper to get the system catalog
RM_SystemCatalog* getSystemCatalog()
{
    return (RM_SystemCatalog *)catalogPageHandle.data;
}

RC markSystemCatalogDirty()
{
    return markDirty(&bufferPool, &catalogPageHandle); 
}

ResourceManagerSchema *getTableByName(char *name)
{
    RM_SystemCatalog *catalog = getSystemCatalog();

    // scan the catalog for the table
    for (int tableIndex = 0; tableIndex < catalog->numTables; tableIndex++)
    {
        ResourceManagerSchema *table = &(catalog->tables[tableIndex]);

        // find the matching name and point the data to the schema handle
        if (strcmp(table->name, name) == 0)
        {
            return table;
        }
    }
    return NULL;
}

// helper to get get page header from a page frame 
RM_PageHeader *getPageHeader(BM_PageHandle* handle)
{
    return (RM_PageHeader *)handle->data;
}

// helper to get slot array from page frame
bool *getSlots(BM_PageHandle* handle)
{
    char *ptr = handle->data;

    // move up the ptr from the header
    ptr += sizeof(RM_PageHeader);
    return (bool *)ptr;
}

// helper to get the tuple data from a page frame
char *getTupleData(BM_PageHandle* handle)
{
    // get start of slot array
    char *ptr = (char *)getSlots(handle);

    // move it down the slot array
    RM_PageHeader *header = getPageHeader(handle);
    ptr += sizeof(bool) * header->numSlots;
    return ptr;
}

ResourceManagerSchema *getSystemSchema(RM_TableData *rel)
{
    return (ResourceManagerSchema *)rel->mgmtData;
}

// helper to get the tuple data at an index from a page frame
char *getTupleDataAt(BM_PageHandle* handle, int recordSize, int index)
{
    // get start of tuple data and move it down
    char *ptr = getTupleData(handle);
    ptr += index * recordSize;
    return ptr;
}

// helper to get the next available free page
// returns the page number of the free page and NO_PAGE for failure
int getFreePage()
{
    USE_PAGE_HANDLE_HEADER(NO_PAGE);

    RM_SystemCatalog *catalog = getSystemCatalog();
    if (catalog->freePage == NO_PAGE)
    {
        int newPage = catalog->totalNumPages++;
        markSystemCatalogDirty();

        // get the new page and unset the next / prev 
        BEGIN_USE_PAGE_HANDLE_HEADER(newPage);
        {
            header->nextPage = header->prevPage = NO_PAGE;
            markDirty(&bufferPool, &handle);
        }
        END_USE_PAGE_HANDLE_HEADER();
        return newPage;
    }
    else
    {
        int newPage = catalog->freePage, nextPage;

        // get the new page's next page, unset the next / prev, and set the catalog to next
        BEGIN_USE_PAGE_HANDLE_HEADER(newPage);
        {
            nextPage = header->nextPage;
            header->nextPage = header->prevPage = NO_PAGE;
            catalog->freePage = nextPage;
            markDirty(&bufferPool, &handle);
            markSystemCatalogDirty();
        }
        END_USE_PAGE_HANDLE_HEADER();
        // if the new page has no next, return
        if (nextPage == NO_PAGE) return newPage;

        // set the next page's prev to the catalog
        BEGIN_USE_PAGE_HANDLE_HEADER(nextPage);
        {
            header->prevPage = 0;
            markDirty(&bufferPool, &handle);
        }
        END_USE_PAGE_HANDLE_HEADER();
        return newPage;
    }
}

// helper to set a page as free by adding it to the free list
// returns 0 for success and 1 for failure
// NOTE the page should not already be in the free list
int setFreePage(BM_PageHandle* handle)
{
    // TODO: implement this for overflow pages
    return 1;
}

// takes a chain of free pages and appends them to the beginning of the free list
// returns 0 for success and 1 for failure
// NOTE the chain should not already be in the free list
int appendToFreeList(int pageNum) 
{
    USE_PAGE_HANDLE_HEADER(1);

    RM_SystemCatalog *catalog = getSystemCatalog();
    if (catalog->freePage == NO_PAGE)
    {
        // the chain becomes the free list 
        BEGIN_USE_PAGE_HANDLE_HEADER(pageNum);
        {
            header->prevPage = 0;
            catalog->freePage = pageNum;
            markDirty(&bufferPool, &handle);
            markSystemCatalogDirty();
        }
        END_USE_PAGE_HANDLE_HEADER();
        return 0;
    }
    else 
    {
        int curPage = pageNum;

        // cycle through the chain until the end
        while (1)
        {
            BEGIN_USE_PAGE_HANDLE_HEADER(curPage);
            {
                // set the last page in the chain to the catalog's next
                if (header->nextPage == NO_PAGE)
                {
                    header->nextPage = catalog->freePage;
                    markDirty(&bufferPool, &handle);
                    END_USE_PAGE_HANDLE_HEADER();
                    break;
                }
                curPage = header->nextPage;
            }
            END_USE_PAGE_HANDLE_HEADER();
        }

        // set the catalog's next's prev to the last page
        BEGIN_USE_PAGE_HANDLE_HEADER(catalog->freePage);
        {
            header->prevPage = curPage;
            markDirty(&bufferPool, &handle);
        }
        END_USE_PAGE_HANDLE_HEADER();

        // set the first page's prev to the catalog and the catalog's next to the first page
        BEGIN_USE_PAGE_HANDLE_HEADER(pageNum)
        {
            header->prevPage = 0;
            catalog->freePage = pageNum;
            markDirty(&bufferPool, &handle);
            markSystemCatalogDirty();
        }
        END_USE_PAGE_HANDLE_HEADER();
        return 0;
    }
}

int getNextPage(ResourceManagerSchema *table, int pageNum)
{
    // keep the main page open
    if (pageNum == table->pageNum)
    {
        return getPageHeader(table->handle)->nextPage;
    }

    int nextPage;
    USE_PAGE_HANDLE_HEADER(NO_PAGE);
    BEGIN_USE_PAGE_HANDLE_HEADER(pageNum);
    {
        nextPage = header->nextPage;
    }
    END_USE_PAGE_HANDLE_HEADER();
    return nextPage;
}


#define BEGIN_SLOT_WALK(table) \
BM_PageHandle *handle = table->handle; \
bool* slots; \
int slotIndex = -1; \
int slotResult = 0;

// call the above macro before initiating the slot walk
// gets the next slotIndex along with the right slots array
// if the page finished, it unpins the page (unless its the main page)
// closeSlots must be called on termination (last page must be manually closed!)
// 0 for success, 1 for failure, -1 for no more slots
int getNextSlotInWalk(ResourceManagerSchema *table, BM_PageHandle **handle, bool** slots, int *slotIndex)
{
    RC result;
    RM_PageHeader *header = getPageHeader(*handle);
    *slots = getSlots(*handle);
    if (*slotIndex < header->numSlots)
    {
        (*slotIndex)++;
        return 0;
    }
    
    *slotIndex = 0;
    int nextPage = header->nextPage;
    if (table->pageNum != (*handle)->pageNum && nextPage != NO_PAGE)
    {
        result = unpinPage(&bufferPool, *handle);
        if (result != RC_OK) return 1;
    }
    if (nextPage == NO_PAGE) return -1; // no more slots
    result = pinPage(&bufferPool, *handle, nextPage);
    if (result != RC_OK) return 1;
    return getNextSlotInWalk(table, handle, slots, slotIndex);
}

int closeSlotWalk(ResourceManagerSchema *table, BM_PageHandle **handle)
{
    if (table->pageNum != (*handle)->pageNum)
    {
        RC result = unpinPage(&bufferPool, *handle);
        if (result != RC_OK) return 1;
    }
}


/* Table and Manager */

RC initRecordManager(void *mgmtData)
{
    // ensure the system catalog can fit into one page
    if (PAGE_SIZE < sizeof(RM_SystemCatalog) || MAX_NUM_TABLES <= 0) return RC_IM_NO_MORE_ENTRIES;

    RC result;
    char *fileName;
    bool newSystem = 0;

    // mgmtData parameter holds the page file name to use (use default name if NULL)
    if (mgmtData == NULL) fileName = PAGE_FILE_NAME;
    else fileName = (char *)mgmtData;

    // check if the file needs to be created
    if (access(fileName, F_OK) != 0)
    {
        result = createPageFile(fileName);
        if (result != RC_OK) return result;
        newSystem = 1;
    }  

    result = initBufferPool(&bufferPool, fileName, 16, RS_LRU, NULL);
    if (result != RC_OK) return result;

    result = pinPage(&bufferPool, &catalogPageHandle, 0);
    if (result != RC_OK) return result;

    // create system schema if it's a new file
    if (newSystem)
    {
        RM_SystemCatalog *catalog = getSystemCatalog();
        catalog->totalNumPages = 1;
        catalog->freePage = NO_PAGE;
        catalog->numTables = 0;
        markSystemCatalogDirty();
    }

    RM_SystemCatalog *catalog = getSystemCatalog();

    return RC_OK;
}

RC shutdownRecordManager()
{
    RC result = unpinPage(&bufferPool, &catalogPageHandle);
    if (result != RC_OK) return result;
    return shutdownBufferPool(&bufferPool);
}

RC createTable(char *name, Schema *schema)
{
    RM_SystemCatalog *catalog = getSystemCatalog();

    // check if table already exists
    if (getTableByName(name) != NULL) return RC_WRITE_FAILED;

    // check if catalog can hold another table and schema is correct
    if (catalog->numTables >= MAX_NUM_TABLES || schema->numAttr > MAX_NUM_ATTR || schema->keySize > MAX_NUM_KEYS) 
    {
        return RC_IM_NO_MORE_ENTRIES;
    }
    ResourceManagerSchema *table = &(catalog->tables[catalog->numTables]);
    strncpy(table->name, name, TABLE_NAME_SIZE - 1);
    table->numTuples = 0;
    table->handle = NULL;

    // copy attribute data
    table->numAttr = schema->numAttr;
    for (int attrIndex = 0; attrIndex < table->numAttr; attrIndex++)
    {
        strncpy(&(table->attrNames[attrIndex * ATTR_NAME_SIZE]), schema->attrNames[attrIndex], ATTR_NAME_SIZE - 1);
        table->dataTypes[attrIndex] = schema->dataTypes[attrIndex];
        table->typeLength[attrIndex] = schema->typeLength[attrIndex];
    }

    // copy key data
    table->keySize = schema->keySize;
    for (int keyIndex = 0; keyIndex < table->keySize; keyIndex++)
    {
        table->keyAttrs[keyIndex] = schema->keyAttrs[keyIndex];
    }
    catalog->numTables++;

    table->pageNum = getFreePage();
    if (table->pageNum == NO_PAGE) return RC_WRITE_FAILED;

    // initialize page
    int pageHeader = sizeof(pageHeader);
    int slotSize = sizeof(bool);
    int recordSize = getRecordSize(schema);
    int recordsPerPage = (PAGE_SIZE - pageHeader) / (recordSize + slotSize);
    if (recordsPerPage <= 0) return RC_WRITE_FAILED;

    USE_PAGE_HANDLE_HEADER(RC_WRITE_FAILED);
    BEGIN_USE_PAGE_HANDLE_HEADER(table->pageNum);
    {
        // mark all the slots as free
        bool *slots = getSlots(&handle);
        header->numSlots = recordsPerPage;
        for (int slotIndex = 0; slotIndex < recordsPerPage; slotIndex++)
        {
            slots[slotIndex] = FALSE;
        }
        markDirty(&bufferPool, &handle);
    }
    END_USE_PAGE_HANDLE_HEADER();

    markSystemCatalogDirty();
    return RC_OK;
}

RC openTable(RM_TableData *rel, char *name)
{
    ResourceManagerSchema *table = getTableByName(name);
    if (table == NULL) return RC_IM_KEY_NOT_FOUND;
    if (table->handle != NULL) return RC_WRITE_FAILED;
    rel->name = table->name;
    rel->schema = (Schema *)malloc(sizeof(Schema));
    rel->schema->attrNames = (char **)malloc(sizeof(char*) * table->numAttr);

    // point to attribute data
    rel->schema->numAttr = table->numAttr;
    for (int attrIndex = 0; attrIndex < table->numAttr; attrIndex++)
    {
        rel->schema->attrNames[attrIndex] = &(table->attrNames[attrIndex * ATTR_NAME_SIZE]);
    }
    rel->schema->dataTypes = table->dataTypes;
    rel->schema->typeLength = table->typeLength;

    // point to key data
    rel->schema->keySize = table->keySize;
    rel->schema->keyAttrs = table->keyAttrs;

    // the RM_TableData will also point to the system schema
    // the system schema stays open until the RM is shut down 
    rel->mgmtData = (void *)table;
    table->handle = (BM_PageHandle *)malloc(sizeof(BM_PageHandle));

    // pin the table's page
    RC result = pinPage(&bufferPool, table->handle, table->pageNum);
    return result;
}

RC closeTable(RM_TableData *rel)
{
    ResourceManagerSchema *table = getSystemSchema(rel);

    // unpin and force the page to disk
    RC result = unpinPage(&bufferPool, table->handle);
    if (result != RC_OK) return result;
    result = forcePage(&bufferPool, table->handle);
    if (result != RC_OK) return result;

    // free mallocs
    free((void *)rel->schema->attrNames);
    free((void *)rel->schema);
    free(table->handle);
    table->handle = NULL;
    return RC_OK;
}

RC deleteTable (char *name)
{
    RM_SystemCatalog *catalog = getSystemCatalog();

    // scan the catalog for the table
    for (int tableIndex = 0; tableIndex < catalog->numTables; tableIndex++)
    {
        ResourceManagerSchema *table = &(catalog->tables[tableIndex]);

        // find the matching name and point the data to the schema handle
        if (strcmp(table->name, name) == 0)
        {
            // put the table's page chain in the free list
            if (appendToFreeList(table->pageNum) == 1) return RC_WRITE_FAILED;

            // shift entries in table catalog down
            catalog->numTables--;
            for (int remainingIndex = tableIndex; remainingIndex < catalog->numTables; remainingIndex++) 
            {
                catalog->tables[remainingIndex] = catalog->tables[remainingIndex + 1];
            }
            markSystemCatalogDirty();
            return RC_OK;
        }
    }
    return RC_IM_KEY_NOT_FOUND;
}

int getNumTuples (RM_TableData *rel)
{
    ResourceManagerSchema *table = getSystemSchema(rel);
    return table->numTuples;
}

/* Manager stats */

int getNumPages()
{
    RM_SystemCatalog *catalog = getSystemCatalog();
    return catalog->totalNumPages;
}

int getNumFreePages()
{
    USE_PAGE_HANDLE_HEADER(0);
    RM_SystemCatalog *catalog = getSystemCatalog();
    int curPage = catalog->freePage;

    // check if there are any free pages
    if (curPage == NO_PAGE) return 0;
    int count = 1;

    // cycle through the chain until the end
    while (1)
    {
        BEGIN_USE_PAGE_HANDLE_HEADER(curPage);
        {
            // set the last page in the chain to the catalog's next
            if (header->nextPage == NO_PAGE)
            {
                count++;
                END_USE_PAGE_HANDLE_HEADER();
                return count;
            }
            curPage = header->nextPage;
        }
        END_USE_PAGE_HANDLE_HEADER();
    }
}

int getNumTables()
{
    RM_SystemCatalog *catalog = getSystemCatalog();
    return catalog->numTables;
}

/* Handling records in a table */

RC insertRecord (RM_TableData *rel, Record *record)
{
    ResourceManagerSchema *table = getSystemSchema(rel); 
    BEGIN_SLOT_WALK(table);
    do
    {
        slotResult = getNextSlotInWalk(table, &handle, &slots, &slotIndex);
        if (slotResult != 0)
        {
            if (slotResult == -1)
            {
                int newPage = getFreePage();
                if (newPage == NO_PAGE) 
                {
                    closeSlotWalk(table, &handle);
                    return RC_WRITE_FAILED;
                }
                RC result = pinPage(&bufferPool, handle, newPage);
                if (result != RC_OK)
                {
                    closeSlotWalk(table, &handle);
                    return result;
                }
                slotIndex = 0;
            }
            else
            {
                closeSlotWalk(table, &handle);
                return RC_WRITE_FAILED;
            }
        }
        if (slots[slotIndex] == FALSE)
        {
            int recordSize = getRecordSize(rel->schema);
            char *tupleData = getTupleDataAt(handle, recordSize, slotIndex);
            memcpy(tupleData, record->data, recordSize);
            slots[slotIndex] = true;
            RC result = markDirty(&bufferPool, handle);
            if (result != RC_OK) return result;
            record->id.page = handle->pageNum;
            record->id.slot = slotIndex;
            table->numTuples++;
            markSystemCatalogDirty();
            closeSlotWalk(table, &handle);
            return RC_OK;
        }
    } while (1);
    closeSlotWalk(table, &handle);
    return RC_WRITE_FAILED;
}

#define BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id) \
ResourceManagerSchema *table = getSystemSchema(rel); \
USE_PAGE_HANDLE_HEADER(RC_WRITE_FAILED); \
if (id.page == table->pageNum) \
{ \
    handle = *table->handle; \
    header = getPageHeader(&handle); \
} \
else \
{ \
    BEGIN_USE_PAGE_HANDLE_HEADER(id.page) \
}

#define END_USE_TABLE_PAGE_HANDLE_HEADER() \
if (id.page != table->pageNum) \
{ \
    END_USE_PAGE_HANDLE_HEADER() \
}

RC deleteRecord (RM_TableData *rel, RID id)
{
    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);
    if (id.slot >= header->numSlots) return RC_WRITE_FAILED;
    bool *slots = getSlots(&handle);
    if (slots[id.slot] == FALSE) return RC_WRITE_FAILED;
    slots[id.slot] = FALSE;
    table->numTuples--;
    markSystemCatalogDirty();
    result = markDirty(&bufferPool, &handle);
    if (result != RC_OK) return RC_WRITE_FAILED;
    END_USE_TABLE_PAGE_HANDLE_HEADER();
    return RC_OK;
}

RC updateRecord (RM_TableData *rel, Record *record)
{
    RID id = record->id;
    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);
    if (id.slot >= header->numSlots) return RC_WRITE_FAILED;
    bool *slots = getSlots(&handle);
    if (slots[id.slot] == FALSE) return RC_WRITE_FAILED;
    int recordSize = getRecordSize(rel->schema);
    char *tupleData = getTupleDataAt(&handle, recordSize, id.slot);
    memcpy(tupleData, record->data, recordSize);
    result = markDirty(&bufferPool, &handle);
    if (result != RC_OK) return RC_WRITE_FAILED;
    END_USE_TABLE_PAGE_HANDLE_HEADER();
    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);
    if (id.slot >= header->numSlots) return RC_WRITE_FAILED;
    bool *slots = getSlots(&handle);
    if (slots[id.slot] == FALSE) return RC_WRITE_FAILED;
    int recordSize = getRecordSize(rel->schema);
    char *tupleData = getTupleDataAt(&handle, recordSize, id.slot);
    memcpy(record->data, tupleData, recordSize);
    record->id.page = id.page;
    record->id.slot = id.slot;
    END_USE_TABLE_PAGE_HANDLE_HEADER();
    return RC_OK;
}

/* Scans */

RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
{
    ResourceManagerSchema *table = getSystemSchema(rel);
    BM_PageHandle *handle = table->handle;
    scan->rel = rel;
    scan->mgmtData = malloc(sizeof(RM_ScanData));
    RM_ScanData *scanData = (RM_ScanData *)scan->mgmtData;
    scanData->id.slot = -1;
    scanData->id.page = handle->pageNum;
    scanData->cond = cond;
    return RC_OK;
}

RC next (RM_ScanHandle *scan, Record *record)
{
    RM_ScanData *scanData = (RM_ScanData *)scan->mgmtData;
    RM_TableData *rel = scan->rel;
    ResourceManagerSchema *table = getSystemSchema(rel);
    BM_PageHandle *handle = table->handle;
    RM_PageHeader *header = getPageHeader(handle);
    bool *slots = getSlots(handle);
    do 
    {
        scanData->id.slot++;
        if (slots[scanData->id.slot] == TRUE)
        {
            RC result = getRecord(rel, scanData->id, record);
            if (result != RC_OK) return result;
            if (scanData->cond == NULL) return RC_OK;

            Value *value;
            result = evalExpr(record, scan->rel->schema, scanData->cond, &value);
            if (result != RC_OK) return result;
            if (value->v.boolV == TRUE)
            {
                freeVal(value);
                return RC_OK;
            }
            else freeVal(value);
        }
    } while (scanData->id.slot < header->numSlots - 1);
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan (RM_ScanHandle *scan)
{
    free(scan->mgmtData);
    return RC_OK;
}

/* Dealing with schemas */

int getAttrSize(Schema *schema, int attrIndex)
{
    switch (schema->dataTypes[attrIndex])
    {
        case DT_INT:
            return sizeof(int);
        case DT_STRING:
            return schema->typeLength[attrIndex] + 1;
        case DT_FLOAT:
            return sizeof(float);
        default:
            return sizeof(bool);
    }
}

int getRecordSize (Schema *schema)
{
    int size = 0;
    for (int attrIndex = 0; attrIndex < schema->numAttr; attrIndex++)
    {
        size += getAttrSize(schema, attrIndex);
    }
    return size;
}

Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
{
    Schema *schema = (Schema *)malloc(sizeof(Schema));
    
    // create attributes
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;

    // create keys
    schema->keyAttrs = keys;
    schema->keySize = keySize;
    return schema;
}

RC freeSchema (Schema *schema)
{
    free((void *)schema);
    return RC_OK;
}

/* Dealing with records and attribute values */

RC createRecord (Record **record, Schema *schema)
{
    int recordSize = getRecordSize(schema);
    *record = (Record *)malloc(sizeof(Record));
    Record *recordPtr = *record;
    recordPtr->data = (char *)malloc(recordSize);
    return RC_OK;
}

RC freeRecord (Record *record)
{
    free(record->data);
    free(record);
    return RC_OK;
}

RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
{
    if (attrNum >= schema->numAttr) return RC_WRITE_FAILED;
    char *dataPtr = record->data;
    for (int attrIndex = 0; attrIndex < attrNum; attrIndex++)
    {
        dataPtr += getAttrSize(schema, attrIndex);
    }
    int attrSize =  getAttrSize(schema, attrNum);
    *value = (Value *)malloc(sizeof(Value));
    Value *valuePtr = *value;
    valuePtr->dt = schema->dataTypes[attrNum];
    switch (valuePtr->dt)
    {
        case DT_INT:
            valuePtr->v.intV = *(int *)dataPtr;
            break;
        case DT_STRING:
            valuePtr->v.stringV = malloc(attrSize);
            memcpy(valuePtr->v.stringV, dataPtr, attrSize);
            break;
        case DT_FLOAT:
            valuePtr->v.floatV = *(float *)dataPtr;
            break;
        default:
            valuePtr->v.boolV = *(bool *)dataPtr;
            break;
    }
    return RC_OK;
}

RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
{
    if (attrNum >= schema->numAttr) return RC_WRITE_FAILED;
    char *dataPtr = record->data;
    for (int attrIndex = 0; attrIndex < attrNum; attrIndex++)
    {
        dataPtr += getAttrSize(schema, attrIndex);
    }
    int attrSize =  getAttrSize(schema, attrNum);
    switch (value->dt)
    {
        case DT_INT:
            memcpy(dataPtr, &(value->v.intV), attrSize);
            break;
        case DT_STRING:
            memcpy(dataPtr, value->v.stringV, attrSize);
            break;
        case DT_FLOAT:
            memcpy(dataPtr, &(value->v.floatV), attrSize);
            break;
        default:
            memcpy(dataPtr, &(value->v.boolV), attrSize);
            break;
    }
    return RC_OK;
}