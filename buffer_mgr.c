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

    switch (result) {
        case RC_OK:
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

        default:
            // Handle all other cases where the page file cannot be opened
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
        int i = 0; // Initialize loop counter for while loop
        while (i < bm->numPages)
        {
            if (pageFrames[i].fixCount > 0) return RC_WRITE_FAILED;
            i++; // Increment loop counter
        }
        
        forceFlushPool(bm);

        i = 0; // Reset loop counter for next while loop
        while (i < bm->numPages)
        {
            // free each page frame's data
            free(pageFrames[i].data);
            i++; // Increment loop counter
        }

        closePageFile(&(metadata->pageFile));

        // free the pageFrames array and metadata
        freeHashTable(pageTabe);
        free(pageFrames);
        free(metadata);
        bm->mgmtData = NULL; // Clear management data pointer
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
        int i = 0; // Initialize loop counter for while loop
        while (i < bm->numPages)
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
            i++; // Increment loop counter
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
        int getValueResult = getValue(pageTabe, page->pageNum, &frameIndex);
        switch (getValueResult) 
        {
            case 0:
                pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

                // set dirty bool
                pageFrames[frameIndex].dirty = true;
                return RC_OK;

            default:
                return RC_IM_KEY_NOT_FOUND;
        }
    }
    else 
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
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
        int getValueResult = getValue(pageTabe, page->pageNum, &frameIndex);
        switch (getValueResult) 
        {
            case 0:
                pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

                // decrement fixCount but ensure it does not drop below 0
                if (pageFrames[frameIndex].fixCount > 0)
                {
                    pageFrames[frameIndex].fixCount--;
                }
                return RC_OK;

            default:
                return RC_IM_KEY_NOT_FOUND;
        }
    }
    else 
    {
        return RC_FILE_HANDLE_NOT_INIT;
    }
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
    // Switch case for checking if management data is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;
            HT_TableHandle *pageTabe = &(metadata->pageTable);
            int frameIndex;

            // make sure the pageNum is not negative
            switch (pageNum >= 0)
            {
                case true:
                {
                    int getValueResult = getValue(pageTabe, pageNum, &frameIndex);
                    switch (getValueResult)
                    {
                        case 0:  // Page is already in a frame
                            pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);
                            pageFrames[frameIndex].fixCount++;
                            page->data = pageFrames[frameIndex].data;
                            page->pageNum = pageNum;
                            return RC_OK;

                        default:  // Page is not in a frame, use replacement strategy
                            BM_PageFrame *pageFrame;
                            switch (bm->strategy)
                            {
                                case RS_FIFO:
                                    pageFrame = replacementFIFO(bm);
                                    break;
                                case RS_LRU:
                                    pageFrame = replacementLRU(bm);
                                    break;
                                default:
                                    return RC_IM_CONFIG_ERROR; // Configuration error if no strategy fits
                            }

                            // Check if the replacement strategy succeeded
                            if (pageFrame == NULL)
                                return RC_WRITE_FAILED;

                            // Successful replacement, setup new frame
                            setValue(pageTabe, pageNum, pageFrame->frameIndex);
                            ensureCapacity(pageNum + 1, &(metadata->pageFile));
                            readBlock(pageNum, &(metadata->pageFile), pageFrame->data);
                            metadata->numRead++;

                            pageFrame->dirty = false;
                            pageFrame->fixCount = 1;
                            pageFrame->occupied = true;
                            pageFrame->pageNum = pageNum;
                            page->data = pageFrame->data;
                            page->pageNum = pageNum;
                            return RC_OK;
                    }
                }
                default:
                    return RC_IM_KEY_NOT_FOUND;  // pageNum is negative
            }
        }
        default:
            return RC_FILE_HANDLE_NOT_INIT;  // Management data not initialized
    }
}

/* Statistics Interface */

PageNumber *getFrameContents (BM_BufferPool *const bm)
{
    // Use switch-case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;

            // Allocate memory for the array; user is responsible for freeing it
            PageNumber *array = (PageNumber *)malloc(sizeof(PageNumber) * bm->numPages);
            int i = 0;  // Initialize loop counter for while loop
            while (i < bm->numPages)
            {
                // Assign page number if frame is occupied, otherwise set to NO_PAGE
                array[i] = pageFrames[i].occupied ? pageFrames[i].pageNum : NO_PAGE;
                i++;  // Increment loop counter
            }
            return array;
        }

        default:
            return NULL;  // Return NULL if management data is not initialized
    }
}

bool *getDirtyFlags (BM_BufferPool *const bm)
{
    // Use switch-case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;

            // Allocate memory for the array; user is responsible for freeing it
            bool *array = (bool *)malloc(sizeof(bool) * bm->numPages);
            int i = 0;  // Initialize loop counter for while loop
            while (i < bm->numPages)
            {
                // Set true if the frame is occupied and dirty, otherwise false
                array[i] = pageFrames[i].occupied ? pageFrames[i].dirty : false;
                i++;  // Increment loop counter
            }
            return array;
        }

        default:
            return NULL;  // Return NULL if management data is not initialized
    }
}

int *getFixCounts (BM_BufferPool *const bm)
{
    // Use switch-case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            BM_PageFrame *pageFrames = metadata->pageFrames;

            // Allocate memory for the array; user is responsible for freeing it
            int *array = (int *)malloc(sizeof(int) * bm->numPages);
            int i = 0;  // Initialize loop counter for while loop
            while (i < bm->numPages)
            {
                // Set the fix count if the frame is occupied, otherwise set to 0
                array[i] = pageFrames[i].occupied ? pageFrames[i].fixCount : 0;
                i++;  // Increment loop counter
            }
            return array;
        }

        default:
            return NULL;  // Return NULL if management data is not initialized
    }
}

int getNumReadIO (BM_BufferPool *const bm)
{
    // Use switch-case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            return metadata->numRead;
        }

        default:
            return 0;  // Return 0 if management data is not initialized
    }
}

int getNumWriteIO (BM_BufferPool *const bm)
{
    // Use switch-case to check if metadata is initialized
    switch (bm->mgmtData != NULL) 
    {
        case true:
        {
            BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
            return metadata->numWrite;
        }

        default:
            return 0;  // Return 0 if management data is not initialized
    }
}


/* Replacement Policies */

BM_PageFrame *replacementFIFO(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    int firstIndex = metadata->queueIndex;
    int currentIndex = firstIndex;

    // Keep cycling in FIFO order until a frame is found that is not pinned
    while (true)
    {
        currentIndex = (currentIndex + 1) % bm->numPages;
        if (pageFrames[currentIndex].fixCount == 0)
            break;
        if (currentIndex == firstIndex)
            break;
    }

    // Update the index back into the metadata
    metadata->queueIndex = currentIndex;

    // Check if we did not cycle into a pinned frame (i.e., all frames are pinned)
    switch (pageFrames[currentIndex].fixCount) 
    {
        case 0:
            return getAfterEviction(bm, currentIndex);
        default:
            return NULL;
    }
}
BM_PageFrame *replacementLRU(BM_BufferPool *const bm)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;

    TimeStamp min = UINT_MAX;
    int minIndex = -1;
    int i = 0;  // Initialize loop counter for while loop

    // Find unpinned frame with smallest timestamp
    while (i < bm->numPages)
    {
        if (pageFrames[i].fixCount == 0 && pageFrames[i].timeStamp < min)
        {
            min = pageFrames[i].timeStamp;
            minIndex = i;
        }
        i++;  // Increment loop counter
    }

    // Use switch-case to handle the case where all frames might be pinned
    switch (minIndex)
    {
        case -1:
            return NULL;  // All frames were pinned
        default:
            return getAfterEviction(bm, minIndex);
    }
}

/* Helpers */

TimeStamp getTimeStamp(BM_Metadata *metadata)
{
    // A switch-case example that logically does nothing different but demonstrates the syntax
    switch (metadata != NULL)
    {
        case true:
            return metadata->timeStamp++;
        default:
            // This case should logically never happen if the function is used correctly
            return 0;  // Returning a default timestamp in case of an error (unexpected)
    }
}

BM_PageFrame *getAfterEviction(BM_BufferPool *const bm, int frameIndex)
{
    BM_Metadata *metadata = (BM_Metadata *)bm->mgmtData;
    BM_PageFrame *pageFrames = metadata->pageFrames;
    HT_TableHandle *pageTabe = &(metadata->pageTable);

    // Update timestamp
    pageFrames[frameIndex].timeStamp = getTimeStamp(metadata);

    // Use switch-case to handle the occupied status of the page frame
    switch (pageFrames[frameIndex].occupied)
    {
        case true:
            // Remove old mapping
            removePair(pageTabe, pageFrames[frameIndex].pageNum);

            // Write old frame back to disk if it's dirty
            switch (pageFrames[frameIndex].dirty)
            {
                case true:
                    writeBlock(pageFrames[frameIndex].pageNum, &(metadata->pageFile), pageFrames[frameIndex].data);
                    metadata->numWrite++;
                    break;
                default:
                    break;
            }
            break;
        default:
            break;
    }

    // Return the evicted frame (caller must deal with setting the page's metadata)
    return &(pageFrames[frameIndex]);
}
