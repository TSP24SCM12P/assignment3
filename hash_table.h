typedef struct HT_TableHandle {
    int size;
    void *mgmt;
} HT_TableHandle;

int initHashTable(HT_TableHandle *const ht, int size);
int getValue(HT_TableHandle *const ht, int key, int *value);
int setValue(HT_TableHandle *const ht, int key, int value);
int removePair(HT_TableHandle *const ht, int key);
void freeHashTable(HT_TableHandle *const ht);