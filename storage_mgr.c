#include "storage_mgr.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

/* manipulating page files */

void initStorageManager(void) { }

typedef enum {
    SUCCESS,
    NULL_POINTER,
    WRITE_ERROR
} FileOperationStatus;

RC createPageFile(char *fileName) {
    FILE *fp = fopen(fileName, "w+");
    FileOperationStatus status;

    if (fp == NULL)
        status = NULL_POINTER;
    else {
        void *emptyPage = malloc(PAGE_SIZE); 
        if (emptyPage == NULL) {
            fclose(fp);
            return 0;  // assuming you handle memory allocation failure separately
        }

        memset(emptyPage, '\0', PAGE_SIZE);
        size_t bytesWritten = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fp);
        fclose(fp);
        free(emptyPage);

        status = (bytesWritten != PAGE_SIZE) ? WRITE_ERROR : SUCCESS;
    }

    switch (status) {
        case NULL_POINTER:
            return RC_FILE_NOT_FOUND;

        case WRITE_ERROR:
            return RC_WRITE_FAILED;

        case SUCCESS:
            return RC_OK;

        default:
            // Optional: Handle unexpected status
            return 0;  // assuming there's a general error return code
    }
}


long _getFileSize(FILE *file)
{
    enum { CHECK_FILE_PTR, CHECK_FTELL_START, DO_FSEEK_END, CHECK_FTELL_END, DO_FSEEK_RESTORE };

    if (file == NULL) {
        goto error;  // Indicate error if the file pointer is null
    }

    // Store the current file position to restore it later
    long current_position = ftell(file);
    if (current_position == -1) {
        goto error;  // Return error if ftell fails
    }

    int action = DO_FSEEK_END;
    switch (action) {
        case DO_FSEEK_END:
            if (fseek(file, 0L, SEEK_END) != 0) {
                goto error;  // Return error if fseek fails
            }

            // Retrieve the current position of the file pointer (now at the end)
            long size = ftell(file);
            if (size == -1) {
                goto error;  // Return error if ftell fails
            }

            action = DO_FSEEK_RESTORE;
            switch (action) {
                case DO_FSEEK_RESTORE:
                    if (fseek(file, current_position, SEEK_SET) != 0) {
                        goto error;  // Return error if fseek fails
                    }
                    return size;  // Return the calculated size

                default:
                    goto error;
            }

        default:
            goto error;
    }

error:
    return -1;
}

typedef enum {
    FILE_SUCCESS,
    FILE_ERROR
} FileStatus;

/**
 * Opens a page file and initializes a file handle structure.
 *
 * @param fileName The name of the file to open.
 * @param fHandle Pointer to the file handle structure to initialize.
 * @return Result code indicating success or failure.
 */
RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    // Attempt to open the file in read+write mode
    FILE *fp = fopen(fileName, "r+");
    FileStatus status = (fp == NULL) ? FILE_ERROR : FILE_SUCCESS;

    switch (status) {
        case FILE_ERROR:
            // If the file pointer is null, return file not found error
            return RC_FILE_NOT_FOUND;

        case FILE_SUCCESS:
            // Get the size of the file and calculate the number of pages
            long fileSize = _getFileSize(fp);
            if (fileSize == -1) {
                fclose(fp);  // Close the file if size retrieval failed
                return RC_FILE_NOT_FOUND;  // Assuming error due to file issues
            }

            int totalNumPages = fileSize / PAGE_SIZE;

            // Set metadata in the file handle
            fHandle->fileName = fileName;
            fHandle->totalNumPages = totalNumPages;
            fHandle->curPagePos = 0;
            fHandle->mgmtInfo = (void *)fp;

            // Successfully opened the file and initialized the file handle
            return RC_OK;

        default:
            // Default case should not be reached
            return 0;  // Fallback error
    }
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;  // Validate file handle and management info pointer
    }

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fclose(fp) == 0) {
        fHandle->mgmtInfo = NULL;  // Unset the file pointer on successful close
        return RC_OK;
    } else {
        return RC_FILE_NOT_FOUND;  // Use a more appropriate error code for fclose failure
    }
}

RC destroyPageFile(char *fileName) {
    if (fileName == NULL) {
        return RC_FILE_NOT_FOUND;  // Return not found if fileName is NULL
    }

    if (remove(fileName) == 0) {
        return RC_OK;  // File successfully deleted
    } else {
        // More precise error checking if needed
             return RC_FILE_NOT_FOUND;  // No such file
    }
}

/* reading blocks from disc */

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;  // Checking for valid file handle and file pointer
    }

    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;  // Page number is out of valid range
    }

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        return RC_FILE_NOT_FOUND;  // fseek failed, possibly due to an invalid file pointer or other issues
    }

    size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, fp);
    if (bytesRead < PAGE_SIZE) {
        if (feof(fp)) {
            return RC_READ_NON_EXISTING_PAGE;  // Attempted to read beyond the end of the file
        } else {
            return RC_READ_FAILED;  // Partial read due to an error
        }
    }

    return RC_OK;  // Successfully read the entire page
}

int getBlockPos (SM_FileHandle *fHandle)
{
    return fHandle->curPagePos;
}

RC readFirstBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL) {
        return RC_FILE_NOT_FOUND;  // Handle the case where the file handle is NULL
    }

    int pageNum = fHandle->curPagePos - 1;

    // Check if the calculated page number is valid
    if (pageNum < 0) {
        return RC_READ_NON_EXISTING_PAGE;  // Previous page number is out of valid range
    }

    RC result = readBlock(pageNum, fHandle, memPage);
    if (result == RC_OK) {
        fHandle->curPagePos = pageNum;  // Update the current page position on successful read
    }

    return result;
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || memPage == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;  // Validate pointers to ensure they are not NULL
    }

    if (fHandle->curPagePos < 0 || fHandle->curPagePos >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;  // Validate current page position
    }

    return readBlock(fHandle->curPagePos, fHandle, memPage);  // Read the block at the current page position
}

RC readNextBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    // get the next `pageNum`
    int pageNum = fHandle->curPagePos + 1;

    RC result = readBlock(pageNum, fHandle, memPage);

    // Using switch case to handle the result
    switch (result) {
        case RC_OK:
            fHandle->curPagePos = pageNum;  // Update `pageNum` on success
            // Fallthrough to default case to return the result
        default:
            return result;  // Return the result of the read operation
    }
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || memPage == NULL) {
        return RC_FILE_NOT_FOUND;  // Validate pointers are not NULL
    }

    if (fHandle->totalNumPages == 0) {
        return RC_READ_NON_EXISTING_PAGE;  // Handle empty file scenario
    }

    int lastPageNum = fHandle->totalNumPages - 1;
    return readBlock(lastPageNum, fHandle, memPage);  // Read the last block
}

/* writing blocks to a page file */

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;  // Check for valid file handle and file pointer
    }

    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_PAGE_OUT_OF_RANGE;  // Page number is out of valid range
    }

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, pageNum * PAGE_SIZE, SEEK_SET) != 0) {
        return RC_FILE_NOT_FOUND;  // fseek failed, possibly due to an invalid file pointer or other issues
    }

    size_t bytesWritten = fwrite(memPage, sizeof(char), PAGE_SIZE, fp);
    if (bytesWritten != PAGE_SIZE) {
        fflush(fp);  // Ensure all I/O operations are flushed in case of partial write
        return RC_WRITE_FAILED;  // Partial write due to an error
    }

    fflush(fp);  // Flush the output buffer to ensure all data is written to disk
    return RC_OK;  // Successfully wrote the entire page
}

RC writeCurrentBlock (SM_FileHandle *fHandle, SM_PageHandle memPage)
{
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}


RC appendEmptyBlock(SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;  // Validate file handle and management info pointer
    }

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    if (fseek(fp, 0, SEEK_END) != 0) {
        return RC_SEEK_FAILED;  // More accurate error code for fseek failure
    }

    void *emptyPage = malloc(PAGE_SIZE);
    if (emptyPage == NULL) {
        return RC_ALLOCATION_FAILED;  // Handle memory allocation failure
    }

    memset(emptyPage, '\0', PAGE_SIZE);
    size_t bytesWritten = fwrite(emptyPage, sizeof(char), PAGE_SIZE, fp);
    free(emptyPage);  // Ensure memory is freed in all cases

    if (bytesWritten != PAGE_SIZE) {
        fflush(fp);  // Flush any written data to handle partial writes
        return RC_WRITE_FAILED;
    }

    fflush(fp);  // Ensure all data is written to disk
    fHandle->totalNumPages++;  // Update the file metadata
    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle == NULL || fHandle->mgmtInfo == NULL) {
        return RC_FILE_NOT_FOUND;  // Check for valid file handle and file pointer
    }

    while (fHandle->totalNumPages < numberOfPages) {
        RC result = appendEmptyBlock(fHandle);
        if (result != RC_OK) {
            return result;  // Return any errors encountered during the append operation
        }
    }
    return RC_OK;  // Successfully ensured the capacity
}