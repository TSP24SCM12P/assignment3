#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* manipulating page files */

void initStorageManager(void) { }


RC createPageFile(char *fileName)
{
    FILE *fp = fopen(fileName, "w+");
    if (fp == NULL) return RC_FILE_NOT_FOUND;
    else
    {
        // allocate a page of memory and fill the page with `\0` bytes
        void *emptyPage = malloc(PAGE_SIZE); 
        memset(emptyPage, '\0', PAGE_SIZE);

        // write the page to disk and close the file
        size_t bytesWritten = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fp);
        fclose(fp);
        free(emptyPage);

        // make sure the page was entirely written
        if (bytesWritten != PAGE_SIZE) return RC_WRITE_FAILED;
        else return RC_OK;
    }
}

long _getFileSize(FILE *file)
{
    // move the file pointer to the end
    fseek(file, 0L, SEEK_END); 

    // find the position of the file pointer from the start of the file
    long size = ftell(file); 

     // move the file pointer back to the beginning
    rewind(file);
    return size;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle)
{
    FILE *fp = fopen(fileName, "r+");

    if (fp == NULL) return RC_FILE_NOT_FOUND;
    else
    {
        // get the size of the file and divide by page size to get `totalNumPages`
        int size = _getFileSize(fp);
        int totalNumPages = size / PAGE_SIZE;
        
        // set metadata
        fHandle->fileName = fileName;
        fHandle->totalNumPages = totalNumPages;
        fHandle->curPagePos = 0;

        // store the file pointer in `mgmtInfo` to use else where
        fHandle->mgmtInfo = (void *)fp;
        return RC_OK;
    }
}

RC closePageFile (SM_FileHandle *fHandle)
{
    if (fclose((FILE *)fHandle->mgmtInfo) == 0)
    {
        // unset the file pointer
        fHandle->mgmtInfo = NULL;
        return RC_OK;
    }
    else return RC_FILE_NOT_FOUND;
}

RC destroyPageFile (char *fileName)
{
    if (remove(fileName) == 0) return RC_OK;
	else return RC_FILE_NOT_FOUND;
}

/* reading blocks from disc */

RC readBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check the handle to see if pageNum is in range
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) 
        return RC_READ_NON_EXISTING_PAGE;
    FILE *fp = (FILE *)fHandle->mgmtInfo;

    // check to see if the position was set successfully
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) == 0)
    {
        size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, fp);

        // make sure the page was entirely read
        if (bytesRead != PAGE_SIZE) return RC_READ_NON_EXISTING_PAGE;
        else return RC_OK;
    }    
    else return RC_FILE_NOT_FOUND;
}

int getBlockPos (SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // get the previous `pageNum`
    int pageNum = fHandle->curPagePos - 1;
    RC result = readBlock(pageNum, fHandle, memPage);

    // if the block was successfully read (and the pageNum was in range), update `pageNum`
    if (result == RC_OK)
        fHandle->curPagePos = pageNum;
    return result;
}

RC readCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // get the next `pageNum`
    int pageNum = fHandle->curPagePos + 1;
    RC result = readBlock(pageNum, fHandle, memPage);

    // if the block was successfully read (and the pageNum was in range), update `pageNum`
    if (result == RC_OK)
        fHandle->curPagePos = pageNum;
    return result;
}

RC readLastBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

/* writing blocks to a page file */

RC writeBlock (int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // check the handle to see if pageNum is in range
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) 
        return RC_READ_NON_EXISTING_PAGE;
    FILE *fp = (FILE *)fHandle->mgmtInfo;

    // check to see if the position was set successfully
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) == 0)
    {
        size_t bytesWritten = fwrite(memPage, sizeof(char), PAGE_SIZE, fp);
        if (bytesWritten != PAGE_SIZE) return RC_WRITE_FAILED;
        else return RC_OK;
    }    
    else return RC_FILE_NOT_FOUND;
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


RC appendEmptyBlock (SM_FileHandle *fHandle)
{
    FILE *fp = (FILE *)fHandle->mgmtInfo;

    // check to see if the position was set successfully
    if (fseek(fp, 0, SEEK_END) == 0) 
    {
        // allocate a page of memory and fill the page with `\0` bytes
        void *emptyPage = malloc(PAGE_SIZE); 
        memset(emptyPage, '\0', PAGE_SIZE);

        // write the page to disk
        size_t bytesWritten = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fp);
        free(emptyPage);

        // make sure the page was entirely written
        if (bytesWritten != PAGE_SIZE) 
            return RC_WRITE_FAILED;
        else
        {
            fHandle->totalNumPages++;
            return RC_OK;
        }
    }
    else return RC_FILE_NOT_FOUND;
}

RC ensureCapacity (int numberOfPages, SM_FileHandle *fHandle)
{
    // while `totalNumPages` is less than `numberOfPages` keep calling `appendEmptyBlock`
    while (fHandle->totalNumPages < numberOfPages)
    {
        RC result = appendEmptyBlock(fHandle);
        if (result != RC_OK) 
            return result;
    }
    return RC_OK;
}