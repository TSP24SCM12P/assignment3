# Assignment 3: Record Manager

- Yousef Suleiman A20463895

## Building

To build the record manager as well the first set of test cases in `test_assign3_1.c`, use

```sh
make
./test_assign3_1.o
```

To build the second set of test cases in `test_assign3_2.c`, use

```sh
make test_assign3_2
./test_assign3_2.o
```

To clean the solution use

```sh
make clean
```

Note that cleaning the solution will also remove the default page file `DATA.bin`.

## Explanation of Solution

The first page in the page file is dedicated to defining the system schema / catalog page . The catalog can be grabbed by casting the raw data of the page to a `RM_SystemCatalog`.

The `RM_SystemCatalog` is a fixed size struct that has 3 `int` metadata as :

- `totalNumPages`: the number of pages the file takes up (this value will only grow)
- `freePage`: and index to the first free page or `NO_PAGE` if there are no free pages
  - free pages are kept track of by a doubly linked list of page pointers on each free pages
- `numTables`: the number of tables in the system
  - the number of tables that can be created is limited by `MAX_NUM_TABLES` 


Lastly, `RM_SystemCatalog` has an array of `ResourceManagerSchema` which defines the system table schemas saved on the catalog page. Its attribute, counts, key counts, and name length are limited so that the page can simply be casted for access (instead of having variable lengths). The `ResourceManagerSchema` also hold a pointer to its page handle in case it is open (or `NULL` if closed).

The page layout has a `RM_PageHeader` which has 3 `int` metadata as:

- `nextPage`
- `prevPage`
- `numSlots`

To keep track of the free list and overflow pages. The number of slots is always fixed allowing the record manager to index into the tuple data after calculating the offset of the header and then the size of each records.

### Table and Manager 

```c
RC initRecordManager(void *mgmtData)
```

- `mgmtData` is used to name the page file used by the record manager. If it is `NULL` the default name `DATA.bin` is used
- if the page file doesn't exist, it is created and the catalog page is initialized  
- starts the buffer pool
- the catalog page is always pinned until the record manager is shutdown

```c
RC shutdownRecordManager()
```

- unpins the catalog page
- shuts down the buffer pool
- if any tables are still open, the buffer pool will not shutdown (as the first page of an open table is pinned until the table is closed)

```c
RC createTable(char *name, Schema *schema)
```

- scans the page catalog for an existing table with `name`. If a table already exists, the operation fails
- makes sure the system is not already at `MAX_NUM_TABLES` and ensures the new schema matches the system's requirements (i.e. attribute & key counts) 
- the table is created (using the first `TABLE_NAME_SIZE` characters)
- the attributes are added to system schema as well
- a free page is taken (either a new page or an existing one from the free list) and its slot array is set to `FALSE` indicating all slots are free

```c
RC openTable(RM_TableData *rel, char *name)
```

- the table is open and its first page is pinned if it exists
- the `mgmtData` of the `RM_TableData` also points back to the system schema

```c
RC closeTable(RM_TableData *rel)
```

- unpins the page, force flushes, then frees the `malloc` from `openTable()`

```c
RC deleteTable (char *name)
```

- must only be called on  a closed table
- given that the table with `name` exists
  - the system schema removes that table from its entry
  - the page (and its overflow pages) are appended onto the free list

### Handling records in a table 

```c
RC insertRecord (RM_TableData *rel, Record *record)
```

- walks down the slots for the table, looking for an opening, starting with the slots on its main page
- overflow pages are then walked onto (these are pinned then immediately unpinned unless a free space is found)
- if there is no free space, a free page is taken (either as a new page or from the free list)

```c
RC deleteRecord (RM_TableData *rel, RID id)
RC updateRecord (RM_TableData *rel, Record *record)
RC getRecord (RM_TableData *rel, RID id, Record *record)
```

- walks into the slot on `id.slot` on page `id.page` and does its required work

### Scans

```c
RC startScan (RM_TableData *rel, RM_ScanHandle *scan, Expr *cond)
RC next (RM_ScanHandle *scan, Record *record)
RC closeScan (RM_ScanHandle *scan)
```

- scans currently only walk the main page of the table (not overflow pages). <span style="color:crimson">This should be fixed but works fine for most test cases</span>

### Dealing with schemas 

```c
int getRecordSize (Schema *schema)
```

- simply adds up the C datatype's length or `typeLength` in case of strings
- this will also add the extra 1 for the `\0` character excluded in  `typeLength`

```c
Schema *createSchema (int numAttr, char **attrNames, DataType *dataTypes, int *typeLength, int keySize, int *keys)
RC freeSchema (Schema *schema)
```

- this is useful for creating tables
- assumes that the caller will deal with `malloc` for relevant data types
- the only thing that is freed is the `Schema` itself

### Dealing with records and attribute values

```c
RC createRecord (Record **record, Schema *schema)
RC freeRecord (Record *record)
```

- allocates memory for the both the record as well as the record's data
- this is useful for creating records on insertion

```c
RC getAttr (Record *record, Schema *schema, int attrNum, Value **value)
```

- this will return a `Value` on the column `attrNum` in the `Record`
- the value's data (and string data if needed) is also `malloc`
- the caller is responsible for calling `freeVal`

```c
RC setAttr (Record *record, Schema *schema, int attrNum, Value *value)
```

- sets the data for an existing `Value` from a `Record` column attribute