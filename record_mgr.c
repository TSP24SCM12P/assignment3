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
    int tableIndex = 0;  // Initialize the loop counter outside the while loop

    // Scan the catalog for the table using a while loop
    while (tableIndex < catalog->numTables)
    {
        ResourceManagerSchema *table = &(catalog->tables[tableIndex]);

        // Find the matching name and point the data to the schema handle
        if (strcmp(table->name, name) == 0)
        {
            return table;
        }

        tableIndex++;  // Manually increment the loop counter
    }
    return NULL;  // Return NULL if no matching table is found
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
    int condition = (catalog->freePage == NO_PAGE) ? 1 : 0;  // Condition for switch-case

    switch (condition)
    {
        case 1:  // No free page available, equivalent to catalog->freePage == NO_PAGE
            {
                int newPage = catalog->totalNumPages++;
                markSystemCatalogDirty();

                // Get the new page and unset the next / prev
                BEGIN_USE_PAGE_HANDLE_HEADER(newPage);
                {
                    header->nextPage = header->prevPage = NO_PAGE;
                    markDirty(&bufferPool, &handle);
                }
                END_USE_PAGE_HANDLE_HEADER();
                return newPage;
            }

        default:  // Free page available, equivalent to else part
            {
                int newPage = catalog->freePage;
                int nextPage;

                // Get the new page's next page, unset the next / prev, and set the catalog to next
                BEGIN_USE_PAGE_HANDLE_HEADER(newPage);
                {
                    nextPage = header->nextPage;
                    header->nextPage = header->prevPage = NO_PAGE;
                    catalog->freePage = nextPage;
                    markDirty(&bufferPool, &handle);
                    markSystemCatalogDirty();
                }
                END_USE_PAGE_HANDLE_HEADER();

                // If the new page has no next, return
                switch (nextPage) 
                {
                    case NO_PAGE:
                        return newPage;

                    default:
                        // Set the next page's prev to NO_PAGE
                        BEGIN_USE_PAGE_HANDLE_HEADER(nextPage);
                        {
                            header->prevPage = NO_PAGE;
                            markDirty(&bufferPool, &handle);
                        }
                        END_USE_PAGE_HANDLE_HEADER();
                        return newPage;
                }
            }
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
    int condition = (catalog->freePage == NO_PAGE) ? 1 : 0;  // Prepare condition for switch-case

    switch (condition)
    {
        case 1:  // Equivalent to catalog->freePage == NO_PAGE
            {
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

        default:  // Equivalent to else part
            {
                int curPage = pageNum;

                // cycle through the chain until the end
                while (1)
                {
                    BEGIN_USE_PAGE_HANDLE_HEADER(curPage);
                    {
                        int nextPageCondition = (header->nextPage == NO_PAGE) ? 1 : 0;
                        switch (nextPageCondition)
                        {
                            case 1:  // There is no next page, we are at the end of the chain
                                header->nextPage = catalog->freePage;
                                markDirty(&bufferPool, &handle);
                                END_USE_PAGE_HANDLE_HEADER();
                                goto end_loop;  // Use goto to break out of the nested switch within the while
                            
                            default:
                                curPage = header->nextPage;
                                break;
                        }
                    }
                    END_USE_PAGE_HANDLE_HEADER();
                }

            end_loop:
                // set the catalog's next's prev to the last page
                BEGIN_USE_PAGE_HANDLE_HEADER(catalog->freePage);
                {
                    header->prevPage = curPage;
                    markDirty(&bufferPool, &handle);
                }
                END_USE_PAGE_HANDLE_HEADER();

                // set the first page's prev to the catalog and the catalog's next to the first page
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
    }
}

int getNextPage(ResourceManagerSchema *table, int pageNum)
{
    // Create a condition that is suitable for using in a switch-case
    int condition = (pageNum == table->pageNum) ? 1 : 0;

    switch (condition)
    {
        case 1:  // True: the page number matches the main page number
            return getPageHeader(table->handle)->nextPage;

        default:  // False: the page number does not match, use a page handle to get the next page
            {
                int nextPage;
                USE_PAGE_HANDLE_HEADER(NO_PAGE);
                BEGIN_USE_PAGE_HANDLE_HEADER(pageNum);
                {
                    nextPage = header->nextPage;
                }
                END_USE_PAGE_HANDLE_HEADER();
                return nextPage;
            }
    }
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
    
    // Use switch-case for slot index comparison
    switch (*slotIndex < header->numSlots)
    {
        case 1:  // True: There are more slots to process in the current page
            (*slotIndex)++;
            return 0;
        default:  // False: No more slots in the current page, need to handle page transition
            break;
    }

    *slotIndex = 0;
    int nextPage = header->nextPage;
    
    // Nested switch-case for checking page transition conditions
    switch (nextPage != NO_PAGE)
    {
        case 1:  // True: There is a next page
            if (table->pageNum != (*handle)->pageNum)
            {
                result = unpinPage(&bufferPool, *handle);
                switch (result)
                {
                    case RC_OK:
                        break;  // Continue if unpin successful
                    default:
                        return 1;  // Error handling if unpin fails
                }
            }
            // Fall through to attempt pinning the next page
            result = pinPage(&bufferPool, *handle, nextPage);
            switch (result)
            {
                case RC_OK:
                    return getNextSlotInWalk(table, handle, slots, slotIndex);  // Recursion to continue the walk
                default:
                    return 1;  // Error handling if pin fails
            }

        default:  // False: No next page, end of slots
            return -1;
    }
}

int closeSlotWalk(ResourceManagerSchema *table, BM_PageHandle **handle)
{
    // Create a condition that is suitable for using in a switch-case
    int condition = (table->pageNum != (*handle)->pageNum) ? 1 : 0;

    switch (condition)
    {
        case 1:
            {
                RC result = unpinPage(&bufferPool, *handle);
                switch (result)
                {
                    case RC_OK:
                        break;  // Successfully unpinned, proceed normally.
                    default:
                        return 1;  // Error in unpinning
                }
            }
            break;
        default:
            break;  // No action needed if page numbers are the same
    }
    
    return 0;  // Indicate successful closure
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
    fileName = (mgmtData == NULL) ? PAGE_FILE_NAME : (char *)mgmtData;

    // check if the file needs to be created using switch-case for file existence check
    switch (access(fileName, F_OK)) {
        case 0:
            // File exists
            break;

        default:
            // File does not exist
            result = createPageFile(fileName);
            if (result != RC_OK) return result;
            newSystem = 1;
            break;
    }

    result = initBufferPool(&bufferPool, fileName, 16, RS_LRU, NULL);
    switch (result) {
        case RC_OK:
            break;
        default:
            return result;
    }

    result = pinPage(&bufferPool, &catalogPageHandle, 0);
    switch (result) {
        case RC_OK:
            break;
        default:
            return result;
    }

    // Create system schema if it's a new file using a switch-case to check newSystem status
    switch (newSystem) {
        case 1:
            {
                RM_SystemCatalog *catalog = getSystemCatalog();
                catalog->totalNumPages = 1;
                catalog->freePage = NO_PAGE;
                catalog->numTables = 0;
                markSystemCatalogDirty();
                break;
            }
        default:
            // No action needed if not a new system
            break;
    }

    RM_SystemCatalog *catalog = getSystemCatalog();  // This might be unused if you follow strict C standards for variable use

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

    // Check if table already exists
    switch ((intptr_t)getTableByName(name)) {
        case (intptr_t)NULL:
            break; // Continue execution if table does not exist
        default:
            return RC_WRITE_FAILED; // Table exists
    }

    // Check if catalog can hold another table and schema is correct
    if (catalog->numTables >= MAX_NUM_TABLES || schema->numAttr > MAX_NUM_ATTR || schema->keySize > MAX_NUM_KEYS) 
    {
        return RC_IM_NO_MORE_ENTRIES;
    }

    ResourceManagerSchema *table = &(catalog->tables[catalog->numTables]);
    strncpy(table->name, name, TABLE_NAME_SIZE - 1);
    table->name[TABLE_NAME_SIZE - 1] = '\0'; // Ensure null termination
    table->numTuples = 0;
    table->handle = NULL;

    // Copy attribute data
    table->numAttr = schema->numAttr;
    int attrIndex = 0;
    while (attrIndex < table->numAttr)
    {
        strncpy(&(table->attrNames[attrIndex * ATTR_NAME_SIZE]), schema->attrNames[attrIndex], ATTR_NAME_SIZE - 1);
        table->attrNames[attrIndex * ATTR_NAME_SIZE + ATTR_NAME_SIZE - 1] = '\0'; // Ensure null termination
        table->dataTypes[attrIndex] = schema->dataTypes[attrIndex];
        table->typeLength[attrIndex] = schema->typeLength[attrIndex];
        attrIndex++;
    }

    // Copy key data
    table->keySize = schema->keySize;
    int keyIndex = 0;
    while (keyIndex < table->keySize)
    {
        table->keyAttrs[keyIndex] = schema->keyAttrs[keyIndex];
        keyIndex++;
    }
    catalog->numTables++;

    table->pageNum = getFreePage();
    if (table->pageNum == NO_PAGE) return RC_WRITE_FAILED;

    // Initialize page
    int pageHeader = sizeof(pageHeader);
    int slotSize = sizeof(bool);
    int recordSize = getRecordSize(schema);
    int recordsPerPage = (PAGE_SIZE - pageHeader) / (recordSize + slotSize);
    if (recordsPerPage <= 0) return RC_WRITE_FAILED;

    USE_PAGE_HANDLE_HEADER(RC_WRITE_FAILED);
    BEGIN_USE_PAGE_HANDLE_HEADER(table->pageNum);
    {
        // Mark all the slots as free
        bool *slots = getSlots(&handle);
        header->numSlots = recordsPerPage;
        int slotIndex = 0;
        while (slotIndex < recordsPerPage)
        {
            slots[slotIndex] = FALSE;
            slotIndex++;
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

    // Using switch-case for initial null check and handle validation
    switch ((intptr_t)table)
    {
        case (intptr_t)NULL:
            return RC_IM_KEY_NOT_FOUND;
        default:
            if (table->handle != NULL) 
                return RC_WRITE_FAILED;
            break;
    }

    rel->name = table->name;
    rel->schema = (Schema *)malloc(sizeof(Schema));
    rel->schema->attrNames = (char **)malloc(sizeof(char*) * table->numAttr);

    // point to attribute data
    rel->schema->numAttr = table->numAttr;
    int attrIndex = 0;
    while (attrIndex < table->numAttr) // Converted for loop to while loop
    {
        rel->schema->attrNames[attrIndex] = &(table->attrNames[attrIndex * ATTR_NAME_SIZE]);
        attrIndex++;
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

    // Unpin the page and force it to disk using switch-case
    RC result = unpinPage(&bufferPool, table->handle);
    switch (result)
    {
        case RC_OK:
            break;  // Continue if everything is okay
        default:
            return result;  // Return the error if any
    }

    result = forcePage(&bufferPool, table->handle);
    switch (result)
    {
        case RC_OK:
            break;  // Continue if everything is okay
        default:
            return result;  // Return the error if any
    }

    // Free allocated memory
    free((void *)rel->schema->attrNames);
    free((void *)rel->schema);
    free(table->handle);
    table->handle = NULL;
    return RC_OK;
}

RC deleteTable (char *name)
{
    RM_SystemCatalog *catalog = getSystemCatalog();
    int tableIndex = 0;

    // scan the catalog for the table using a while loop
    while (tableIndex < catalog->numTables)
    {
        ResourceManagerSchema *table = &(catalog->tables[tableIndex]);
        int nameMatch = strcmp(table->name, name); // Store comparison result

        // Use switch-case to handle matching name case
        switch (nameMatch)
        {
            case 0:  // Name matches
                {
                    int appendResult = appendToFreeList(table->pageNum);
                    switch (appendResult)
                    {
                        case 1:
                            return RC_WRITE_FAILED;

                        default:
                            // Shift entries in the table catalog down
                            catalog->numTables--;
                            for (int remainingIndex = tableIndex; remainingIndex < catalog->numTables; remainingIndex++) 
                            {
                                catalog->tables[remainingIndex] = catalog->tables[remainingIndex + 1];
                            }
                            markSystemCatalogDirty();
                            return RC_OK;
                    }
                }

            default:
                // Name does not match, continue scanning
                break;
        }
        tableIndex++;
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

    // Prepare to use a switch-case for checking if there are any free pages
    switch (curPage) {
        case NO_PAGE:
            return 0;
        default:
            break;
    }

    int count = 1;

    // Changed while loop to do-while loop
    do
    {
        BEGIN_USE_PAGE_HANDLE_HEADER(curPage);
        // Check if this is the last page in the chain
        switch (header->nextPage) {
            case NO_PAGE:
                count++;
                END_USE_PAGE_HANDLE_HEADER();
                return count;

            default:
                curPage = header->nextPage;
                break;
        }
        END_USE_PAGE_HANDLE_HEADER();
    } while (1);
}

int getNumTables()
{
    RM_SystemCatalog *catalog = getSystemCatalog();
    return catalog->numTables;
}

/* Handling records in a table */
/* Handling records in a table */
RC insertRecord (RM_TableData *rel, Record *record)
{
    ResourceManagerSchema *schema = getSystemSchema(rel);
    bool *slotAvailability;
    BEGIN_SLOT_WALK(schema);

    while (true)
    {
        int slotResult = getNextSlotInWalk(schema, &handle, &slotAvailability, &slotIndex);
        if (slotResult != 0)
        {
            if (slotResult == -1)
            {
                int newPage = getFreePage();
                if (newPage == NO_PAGE) 
                {
                    closeSlotWalk(schema, &handle);
                    return RC_WRITE_FAILED;
                }
                RC result = pinPage(&bufferPool, handle, newPage);
                if (result != RC_OK)
                {
                    closeSlotWalk(schema, &handle);
                    return result;
                }
                slotIndex = 0;
            }
            else
            {
                closeSlotWalk(schema, &handle);
                return RC_WRITE_FAILED;
            }
        }
        if (!slotAvailability[slotIndex])
        {
            int recordSize = getRecordSize(rel->schema);
            char *tupleData = getTupleDataAt(handle, recordSize, slotIndex);
            memcpy(tupleData, record->data, recordSize);
            slotAvailability[slotIndex] = true;
            RC result = markDirty(&bufferPool, handle);
            if (result != RC_OK) 
            {
                return result;
            }
            record->id.page = handle->pageNum;
            record->id.slot = slotIndex;
            schema->numTuples++;
            markSystemCatalogDirty();
            closeSlotWalk(schema, &handle);
            return RC_OK;
        }
    }
    closeSlotWalk(schema, &handle);
    return RC_WRITE_FAILED;
}

// baki uppar wadu 

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

typedef enum {
    CHECK_SLOT_RANGE,
    CHECK_SLOT_USAGE,
    CHECK_MARK_DIRTY
} ErrorCode;

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

    // Validate slot range and usage
    if (id.slot >= header->numSlots) 
        return RC_WRITE_FAILED;

    bool *slots = getSlots(&handle);
    if (!slots[id.slot]) 
        return RC_WRITE_FAILED;

    // Update record in the slot
    int recordSize = getRecordSize(rel->schema);
    char *tupleData = getTupleDataAt(&handle, recordSize, id.slot);
    memcpy(tupleData, record->data, recordSize);

    // Mark the page as dirty
    result = markDirty(&bufferPool, &handle);
    if (result != RC_OK) 
        return result;

    END_USE_TABLE_PAGE_HANDLE_HEADER();
    return RC_OK;
}

RC getRecord (RM_TableData *rel, RID id, Record *record)
{
    BEGIN_USE_TABLE_PAGE_HANDLE_HEADER(id);

    // Check if the slot index is valid
    if (id.slot >= header->numSlots) 
        return RC_WRITE_FAILED;

    bool *slots = getSlots(&handle);

    // Check if the slot is in use
    if (!slots[id.slot])
        return RC_WRITE_FAILED;

    // Retrieve data
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

    while (scanData->id.slot < header->numSlots - 1)
    {
        scanData->id.slot++;
        if (slots[scanData->id.slot])
        {
            RC result = getRecord(rel, scanData->id, record);
            if (result != RC_OK) 
                return result;

            if (scanData->cond == NULL) 
                return RC_OK;

            Value *value;
            result = evalExpr(record, scan->rel->schema, scanData->cond, &value);
            if (result != RC_OK) 
                return result;

            if (value->v.boolV)
            {
                freeVal(value);
                return RC_OK;
            }
            freeVal(value);
        }
    }
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
    DataType type = schema->dataTypes[attrIndex];  // Get the type of the attribute

    if (type == DT_INT)
    {
        return sizeof(int);  // Return the size of int
    }
    else if (type == DT_STRING)
    {
        return schema->typeLength[attrIndex] + 1;  // Return the size of the string type plus null terminator
    }
    else if (type == DT_FLOAT)
    {
        return sizeof(float);  // Return the size of float
    }
    else
    {
        return sizeof(bool);  // Default case to handle boolean and any other types
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
    if (attrNum >= schema->numAttr) 
        return RC_WRITE_FAILED;

    char *dataPtr = record->data;
    int attrIndex = 0;
    while (attrIndex < attrNum)
    {
        dataPtr += getAttrSize(schema, attrIndex);
        attrIndex++;
    }

    int attrSize = getAttrSize(schema, attrNum);
    *value = (Value *)malloc(sizeof(Value));
    Value *valuePtr = *value;
    valuePtr->dt = schema->dataTypes[attrNum];

    if (valuePtr->dt == DT_INT)
    {
        valuePtr->v.intV = *(int *)dataPtr;
    }
    else if (valuePtr->dt == DT_STRING)
    {
        valuePtr->v.stringV = malloc(attrSize + 1); // plus one for null-termination if needed
        memcpy(valuePtr->v.stringV, dataPtr, attrSize);
        valuePtr->v.stringV[attrSize] = '\0'; // ensure null-terminated string
    }
    else if (valuePtr->dt == DT_FLOAT)
    {
        valuePtr->v.floatV = *(float *)dataPtr;
    }
    else  // Assuming it must be DT_BOOL
    {
        valuePtr->v.boolV = *(bool *)dataPtr;
    }

    return RC_OK;
}


RC setAttr(Record *record, Schema *schema, int attrNum, Value *value)
{
    if (attrNum >= schema->numAttr) 
        return RC_WRITE_FAILED;

    char *dataPtr = record->data;
    int attrIndex = 0;
    while (attrIndex < attrNum)
    {
        dataPtr += getAttrSize(schema, attrIndex);
        attrIndex++;
    }

    int attrSize = getAttrSize(schema, attrNum);

    if (value->dt == DT_INT)
    {
        memcpy(dataPtr, &(value->v.intV), attrSize);
    }
    else if (value->dt == DT_STRING)
    {
        memcpy(dataPtr, value->v.stringV, attrSize);
    }
    else if (value->dt == DT_FLOAT)
    {
        memcpy(dataPtr, &(value->v.floatV), attrSize);
    }
    else  // Assuming it must be DT_BOOL
    {
        memcpy(dataPtr, &(value->v.boolV), attrSize);
    }

    return RC_OK;
}
