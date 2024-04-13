#include "buffer_mgr.h"
#include "storage_mgr.h"
#include "hash_table.h"
#include <stdlib.h>
#include <limits.h>

/* Additional Definitions */

#define PAGE_TABLE_SIZE 256

typedef unsigned int TimeStamp;

typedef struct BM_PageFrame {
    // the frame's buffer
    char* data;
    // the page currently occupying it
    PageNumber pageNum;
    // management data on the page frame
    int frameIndex;
    int fixCount;
    bool dirty;
    bool occupied;
    TimeStamp timeStamp;
} BM_PageFrame;

typedef struct BM_Metadata {
    // an array of frames
    BM_PageFrame *pageFrames;
    // a page table that associates the a page ID with an index in pageFrames
    HT_TableHandle pageTable;
    // the file handle
    SM_FileHandle pageFile;
    // increments everytime a page is accessed (used for frame's timeStamp)
    TimeStamp timeStamp;
    // used to treat *pageFrames as a queue
    int queueIndex;
    // statistics
    int numRead;
    int numWrite;
} BM_Metadata;

/* Declarations */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm);

BM_PageFrame *replacementLRU(BM_BufferPool *const bm);

// use this helper to increment the pool's global timestamp and return it
TimeStamp getTimeStamp(BM_Metadata *metadata);

// use this help to evict the frame at frameIndex (write if occupied and dirty) and return the new empty frame
BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex);

/* Buffer Manager Interface Pool Handling */

RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData)
{
    // initialize the metadata
    BM_Metadata *metadata = (BM_Metadata *)malloc(sizeof(BM_Metadata));
    HT_TableHandle *pageTabe = &(metadata->pageTable);
    metadata->timeStamp = 0;

    // start the queue from the last element as it gets incremented by one and modded 
    // at the start of each call of replacementFIFO
    metadata->queueIndex = bm->numPages - 1;
    metadata->numRead = 0;
    metadata->numWrite = 0;
    RC result = openPageFile((char *)pageFileName, &(metadata->pageFile));
    if (result == RC_OK)
    {
        initHashTable(pageTabe, PAGE_TABLE_SIZE);
        metadata->pageFrames = (BM_PageFrame *)malloc(sizeof(BM_PageFrame) * numPages);
        for (int i = 0; i < numPages; i++)
        {
            metadata->pageFrames[i].frameIndex = i;
            metadata->pageFrames[i].data = (char *)malloc(PAGE_SIZE);
            metadata->pageFrames[i].fixCount = 0;
            metadata->pageFrames[i].dirty = false;
            metadata->pageFrames[i].occupied = false;
            metadata->pageFrames[i].timeStamp = getTimeStamp(metadata);
        }
        bm->mgmtData = (void *)metadata;
        bm->numPages = numPages;
        bm->pageFile = (char *)&(metadata->pageFile);
        bm->strategy = strategy;
        return RC_OK;
    }
    else
    {
        // in case the file can't be open, set the metadata to NULL
        bm->mgmtData = NULL;
        return result;
    }
}

RC shutdownBufferPool(BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTable);
        
        // "It is an error to shutdown a buffer pool that has pinned pages."
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].fixCount > 0) return RC_WRITE_FAILED;
        }
        forceFlushPool(bm);
        for (int i = 0; i < bm->numPages; i++)
        {
            // free each page frame's data
            free(pageFrames[i].data);
        }
        closePageFile(&(metadata->pageFile));

        // free the pageFrames array and metadata
        freeHashTable(pageTabe);
        free(pageFrames);
        free(metadata);
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC forceFlushPool(BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        for (int i = 0; i < bm->numPages; i++)
        {
            // write the occupied, dirty, and unpinned pages to disk
            if (pageFrames[i].occupied && pageFrames[i].dirty && pageFrames[i].fixCount == 0)
            {
                writeBlock(pageFrames[i].pageNum, &(metadata->pageFile), pageFrames[i].data);
                metadata->numWrite++;
                pageFrames[i].timeStamp = getTimeStamp(metadata);

                // clear the dirty bool
                pageFrames[i].dirty = false;
            }
        }
        return RC_OK;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Buffer Manager Interface Access Pages */

RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTable);
        int frameIndex;

        // get the mapped frameIndex from pageNum
        if (getValue(pageTabe, page->pageNum, &frameIndex) == 0)
        {
            pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

            // set dirty bool
            pageFrames[frameIndex].dirty = true;
            return RC_OK;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTable);
        int frameIndex;

        // get the mapped frameIndex from pageNum
        if (getValue(pageTabe, page->pageNum, &frameIndex) == 0)
        {
            pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

            // decrement (not below 0)
            pageFrames[frameIndex].fixCount--;
            if (pageFrames[frameIndex].fixCount < 0)
                pageFrames[frameIndex].fixCount = 0;
            return RC_OK;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTable);
        int frameIndex;

        // get the mapped frameIndex from pageNum
        if (getValue(pageTabe, page->pageNum, &frameIndex) == 0)
        {
            pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

            // only force the page if it is not pinned
            if (pageFrames[frameIndex].fixCount == 0)
            {
                writeBlock(page->pageNum, &(metadata->pageFile), pageFrames[frameIndex].data);
                metadata->numWrite++;

                // clear dirty bool
                pageFrames[frameIndex].dirty = false;
                return RC_OK;
            }
            else return RC_WRITE_FAILED;
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum)
{
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;
        HT_TableHandle *pageTabe = &(metadata->pageTable);
        int frameIndex;

        // make sure the pageNum is not negative
        if (pageNum >= 0) 
        {
            // check if page is already in a frame and get the mapped frameIndex from pageNum
            if (getValue(pageTabe, pageNum, &frameIndex) == 0)
            {
                pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);
                pageFrames[frameIndex].fixCount++;
                page->data = pageFrames[frameIndex].data;
                page->pageNum = pageNum;
                return RC_OK;
            }
            else 
            {
                // use specified replacement strategy
                BM_PageFrame *pageFrame;
                if (bm->strategy == RS_FIFO)
                    pageFrame = replacementFIFO(bm);
                else // if (bm->strategy == RS_LRU)
                    pageFrame = replacementLRU(bm);

                // if the strategy failed (i.e. all frames are pinned) return error
                if (pageFrame == NULL)
                    return RC_WRITE_FAILED;
                else 
                {
                    // set the mapping from pageNum to frameIndex
                    setValue(pageTabe, pageNum, pageFrame->frameIndex);

                    // grow the file if needed
                    ensureCapacity(pageNum + 1, &(metadata->pageFile));

                    // read data from disk
                    readBlock(pageNum, &(metadata->pageFile), pageFrame->data);
                    metadata->numRead++;

                    // set frame's metadata
                    pageFrame->dirty = false;
                    pageFrame->fixCount = 1;
                    pageFrame->occupied = true;
                    pageFrame->pageNum = pageNum;
                    page->data = pageFrame->data;
                    page->pageNum = pageNum;
                    return RC_OK;
                }
            }
        }
        else return RC_IM_KEY_NOT_FOUND;
    }
    else return RC_FILE_HANDLE_NOT_INIT;
}

/* Statistics Interface */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].occupied)
                array[i] = pageFrames[i].pageNum;
            else array[i] = NO_PAGE;
        }
        return array;
    }
    else return NULL;
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].occupied)
                array[i] = pageFrames[i].dirty;
            else array[i] = false;
        }
        return array;
    }
    else return NULL;
}

int *getFixCounts (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        BM_PageFrame *pageFrames = metadata->pageFrames;

        // the user will be responsible for calling free
        int *array = (int *)malloc(sizeof(int) * bm->numPages);
        for (int i = 0; i < bm->numPages; i++)
        {
            if (pageFrames[i].occupied)
                array[i] = pageFrames[i].fixCount;
            else array[i] = 0;
        }
        return array;
    }
    else return NULL;
}

int getNumReadIO (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        return metadata->numRead;
    }
    else return 0;
}

int getNumWriteIO (BM_BufferPool *const bm)
{
    // make sure the metadata was successfully initialized
    if (bm->mgmtData != NULL) 
    {
        BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
        return metadata->numWrite;
    }
    else return 0;
}

/* Replacement Policies */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int firstIndex = metadata->queueIndex;
    int currentIndex = metadata->queueIndex;

    // keep cycling in FIFO order until a frame is found that is not pinned
    do 
    {
        currentIndex = (currentIndex + 1) % bm->numPages;
        if (pageFrames[currentIndex].fixCount == 0)
            break;
    }
    while (currentIndex != firstIndex);

    // put the index back into the metadata pointer
    metadata->queueIndex = currentIndex;

    // ensure we did not cycle into a pinned frame (i.e. all frames are pinned) or return NULL
    if (pageFrames[currentIndex].fixCount == 0)
        return getAfterEviction(bm, currentIndex);
    else return NULL;
}

BM_PageFrame *replacementLRU(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    TimeStamp min = UINT_MAX;
    int minIndex = -1;

    // find unpinned frame with smallest timestamp
    for (int i = 0; i < bm->numPages; i++)
    {
        if (pageFrames[i].fixCount == 0 && pageFrames[i].timeStamp < min) 
        {
            min = pageFrames[i].timeStamp;
            minIndex = i;
        }
    }
    
    // if all frames were pinned, return NULL
    if (minIndex == -1) 
        return NULL;
    else return getAfterEviction(bm, minIndex);
}

/* Helpers */

TimeStamp getTimeStamp(BM_Metadata *metadata)
{
    // increment the global timestamp after returning it to be assigned to a frame
    return metadata->timeStamp++;
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTabe = &(metadata->pageTable);

    // update timestamp
    pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);
    if (pageFrames[frameIndex].occupied)
    {
        // remove old mapping
        removePair(pageTabe, pageFrames[frameIndex].pageNum);

        // write old frame back to disk if dirty
        if (pageFrames[frameIndex].dirty) 
        {
            writeBlock(pageFrames[frameIndex].pageNum, &(metadata->pageFile), pageFrames[frameIndex].data);
            metadata->numWrite++;
        }
    }

    // return evicted frame (called must deal with setting the page's metadata)
    return &(pageFrames[frameIndex]);
}