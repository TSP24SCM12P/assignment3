#include "hash_table.h"
#include <stdlib.h>

#define ARRAY_LIST_SIZE 16

typedef struct HT_KeyValuePair {
    int key;
    int value;
} HT_KeyValuePair;

typedef struct HT_ArrayList {
    int capacity;
    int size;
    HT_KeyValuePair *list;
} HT_ArrayList;

HT_ArrayList *AL_get(HT_TableHandle *const ht, int i)
{
    HT_ArrayList *ls = (HT_ArrayList *)ht->mgmt;
    return &ls[i];
}

int AL_push(HT_ArrayList *al, int key, int value)
{
    if (al->size == al->capacity)
    {
        al->capacity += ARRAY_LIST_SIZE;
        al->list = realloc(al->list, sizeof(HT_KeyValuePair) * al->capacity);
        if (al->list == NULL)
            return 1;
    }
    al->list[al->size].key = key;
    al->list[al->size].value = value;
    al->size++;
    return 0;
}

void AL_remoteAt(HT_ArrayList *al, int i)
{
    if (i >= 0 && i < al->size) {
        for (int j = i; j < al->size - 1; j++)
            al->list[j] = al->list[j + 1];
        al->size--;
    }
}

// initialize hash table
int initHashTable(HT_TableHandle *const ht, int size) 
{
    ht->size = size;
    ht->mgmt = malloc(sizeof(HT_ArrayList) * size);
    for (int i = 0; i < size; i++)
    {
        HT_ArrayList *al = AL_get(ht, i);
        al->list = malloc(sizeof(HT_KeyValuePair) * ARRAY_LIST_SIZE);
        if (al->list == NULL)
            return 1;
        al->capacity = ARRAY_LIST_SIZE;
        al->size = 0;
    }
    return 0;
}

// if the key is found then assign to value and return 0
// else return 1
int getValue(HT_TableHandle *const ht, int key, int *value) 
{
    int i = key % ht->size;
    HT_ArrayList *al  = AL_get(ht, i);
    for (int j = 0; j < al->size; j++)
    {
        if (al->list[j].key == key)
        {
            *value = al->list[j].value;
            return 0;
        }
    }
    return 1;
}

// if the key exists, then assign value to it
// else, add in a new HT_KeyValuePair
int setValue(HT_TableHandle *const ht, int key, int value) 
{
    int i = key % ht->size;
    HT_ArrayList *al  = AL_get(ht, i);
    for (int j = 0; j < al->size; j++)
    {
        if (al->list[j].key == key)
        {
            al->list[j].value = value;
            return 0;
        }
    }
    return AL_push(al, key, value);
}

// remove a HT_KeyValuePair 
int removePair(HT_TableHandle *const ht, int key) 
{
    int i = key % ht->size;
    HT_ArrayList *al  = AL_get(ht, i);
    for (int j = 0; j < al->size; j++)
    {
        if (al->list[j].key == key)
        {
            AL_remoteAt(al, j);
            return 0;
        }
    }
    return 1;
}

// free malloc's
void freeHashTable(HT_TableHandle *const ht)
{
    for (int i = 0; i < ht->size; i++)
    {
        HT_ArrayList *al = AL_get(ht, i);
        free(al->list);
    }
    free(ht->mgmt);
}