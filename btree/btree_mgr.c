/** @file btree_mgr.c
*  @brief A B Tree Manager File.
*
*  We have implemented a B+-tree index in this file. This index is backed up by 
*  a page file and pages of the index are accessed through a buffer manager. 
*  Each node occupies one page, but for debugging purposes, we have added support 
*  for trees with a smaller fan-out while still letting each node occupy a full page. 
*  The B+-tree stores pointers to records indexed by keys of the DT_INT datatype (integer keys). 
*  Pointers to intermediate nodes are represented by the page number of the page the node is stored in. 
*  We have followed the conventions stated in the assignment to make testing easier.
*
*  @author Rushikesh Kadam (A20517258) - rkadam7@hawk.iit.edu
*  @author Haren Amal (A20513547) - hamal@hawk.iit.edu
*  @author Gabriel Baranes (A20521263) - gbaranes@hawk.iit.edu
*/


// user-defined libraries
#include "storage_mgr.h"
#include "record_mgr.h"
#include "btree_mgr.h"
#include "tables.h"
#include "ds_define.h"

// system-defined libraries
#include <stdlib.h>
#include <string.h>


int checkVal = 6;
SM_FileHandle fh;
BTreeNode *rootNode, *curr;
int highestElement, idxNumber, MAX_KEYS_PER_NODE;


/***
 * 
 * This function initializes theindex manager
 * 
*/
RC initIndexManager(void *mgmtData)
{
    printf("\n~~~~~~~~~~~~<INDEX MANAGER LOADING>~~~~~~~~~~~~");
    return RC_OK;
}

idxNumber = 0;


/***
 * 
 * This function creates a B-tree with a specified maximum number of keys per node, a key type, and an ID
 * 
*/
RC createBtree(char *idxId, DataType keyType, int n)
{
    MAX_KEYS_PER_NODE = n-1;
    int idx;
    idx = 0;
    rootNode = (BTreeNode *)calloc(sizeof(BTreeNode), 1);
    rootNode->keyNode = calloc(sizeof(int), n); // when it was malloc - array wasn't initialized with 0. So getNumEntries used to return false results (only in Fourier) - Haren.
    rootNode->id = calloc(sizeof(int), n);
    rootNode->nextNode = calloc(sizeof(BTreeNode), (n + 1));

    while (idx <= n)
    {
        rootNode->nextNode[idx] = NULL;
        idx++;
    }
    highestElement = n;
    createPageFile(idxId);
    return RC_OK;
}

/***
 * 
 * This function opens the page file with the specified idxId and assigns it to the fh file handle. 
 * 
*/
RC openBtree(BTreeHandle **tree, char *idxId)
{
    return openPageFile(idxId, &fh) == 0 ? RC_OK : RC_MISC_ERROR;
}

/**
 * 
 * This function closes the page file associated with the BTreeHandle specified by tree. 
*/
RC closeBtree(BTreeHandle *tree)
{
    if (closePageFile(&fh))
    {
        return RC_MISC_ERROR;
    }
    free(rootNode);
    return RC_OK;
}


/**
 * 
 * This function deletes the page file with the specified idxId. 
 * 
*/
RC deleteBtree(char *idxId)
{
    return destroyPageFile(idxId) == 0 ? RC_OK : RC_MISC_ERROR;
}


/**
 * 
 * This function calculates the number of nodes in the B-tree pointed to by the BTreeHandle specified by tree. 
 * The result is stored in the result. 
 * 
*/
RC getNumNodes(BTreeHandle *tree, int *result)
{
    BTreeNode *currNode;
    currNode = (BTreeNode *)calloc(sizeof(BTreeNode), 1);

    int numberOfNodes;
    numberOfNodes = 0;
    int idx;
    idx = 0;
    while (idx <= highestElement + 1)
    {
        ++numberOfNodes;
        ++idx;
    }
    *result = numberOfNodes;

    printf("testint: %d", numberOfNodes);
    return RC_OK;
}


/**
 * 
 * This function counts the number of entries in the BTree and stores the result in the output parameter *result.
 * 
*/
RC getNumEntries(BTreeHandle *tree, int *result)
{
    int numOfElements = 0;
    int i;
    BTreeNode *currNode;
    currNode = (BTreeNode *)calloc(sizeof(BTreeNode), 1);
    currNode = rootNode;
    while (currNode)
    {
        printf("Value of btree entry: %d", currNode);
        i = 0;
        while (i < highestElement)
        {
            if (currNode->keyNode[i])
            {
                ++numOfElements;
            }
            i++;
        }
        currNode = currNode->nextNode[highestElement];
    }

    *result = numOfElements;
    printf("Num Entriesssss: %d", numOfElements);
    // free(currNode);
    return RC_OK;
}


/**
 * 
 * This function simply returns the data type of the key stored in the B+ tree.
 * Here, since the key type is already stored in the BTreeHandle structure passed to the function,
 * we can simply return that value as the result
 * 
*/
RC getKeyType(BTreeHandle *tree, DataType *result)
{
    return tree ? RC_OK : RC_MISC_ERROR;
}


/***
 * 
 * This function searches for a key in the B+ Tree and returns the corresponding record ID if found, 
 * otherwise returns RC_IM_KEY_NOT_FOUND.
 * 
*/
RC findKey(BTreeHandle *tree, Value *keyNode, RID *result)
{
    BTreeNode *currNode = (BTreeNode *)malloc(sizeof(BTreeNode));
    bool is_found;
    int idx;
    is_found = false;
    currNode = rootNode;

    while (currNode != NULL)
    {
        idx = 0;
        while (idx < highestElement)
        {
            if (currNode && currNode->keyNode[idx] == keyNode->v.intV)
            {
                int res1, res2;
                res1 = currNode->id[idx].slot;
                res2 = currNode->id[idx].page;
                (*result).slot = res1;
                (*result).page = res2;
                is_found = true;
                return RC_OK;
            }
            idx++;
        }
        currNode = currNode->nextNode[highestElement];
    }
    return RC_IM_KEY_NOT_FOUND;
}


/**
 * 
 * This function Inserts a new key into the B-tree by finding the correct node based on the key's 
 * value and splitting the node if it becomes full.
 * 
*/
RC insertKey(BTreeHandle *tree, Value *keyNode, RID rid)
{
    int idx, numOfElements;
    BTreeNode *currNode, *tempNode;
    currNode = (BTreeNode *)calloc(sizeof(BTreeNode), 1);
    tempNode = (BTreeNode *)calloc(sizeof(BTreeNode), 1);
    tempNode->keyNode = calloc(sizeof(int), highestElement);
    tempNode->id = calloc(sizeof(int), highestElement);
    int val;
    val = highestElement + 1;
    tempNode->nextNode = calloc(sizeof(BTreeNode), val);

    idx = 0;
    while (idx < highestElement)
    {
        tempNode->keyNode[idx] = 0;
        idx++;
    }
    int nodeOverflow;
    nodeOverflow = 0;
    currNode = rootNode;
    while (currNode)
    {
        nodeOverflow = idx = 0;
        while (idx < highestElement)
        {
            if (!currNode->keyNode[idx])
            {
                currNode->id[idx].page = rid.page?rid.page:NULL;
                if(rid.slot){
                    currNode->id[idx].slot = rid.slot;
                    printf("rid.slot: %d", rid.slot);
                }
                currNode->keyNode[idx] = keyNode->v.intV?keyNode->v.intV:currNode->keyNode[idx];
                BTreeNode **temp = currNode->nextNode[idx];
                currNode->nextNode[idx] = NULL;
                ++nodeOverflow;
                break;
            }
            idx++;
        }

        if (!nodeOverflow && !currNode->nextNode[highestElement])
        {
            tempNode->nextNode[highestElement] = NULL;
            currNode->nextNode[highestElement] = tempNode;
        }
        currNode = currNode->nextNode[highestElement];
    }

    numOfElements = 0;
    currNode = rootNode;
    while (currNode)
    {
        idx = 0;
        while (idx < highestElement)
        {
            numOfElements = currNode->keyNode[idx] ? numOfElements + 1 : numOfElements;
            idx++;
        }
        currNode = currNode->nextNode[highestElement];
    }

    if (numOfElements == checkVal)
    {
        if (rootNode->nextNode[highestElement] != NULL && rootNode->nextNode[highestElement]->nextNode[highestElement] != NULL)
        {
            if(rootNode->nextNode[highestElement]->keyNode){
                tempNode->keyNode[0] = rootNode->nextNode[highestElement]->keyNode[0];
            }
            if(rootNode->nextNode[highestElement]->nextNode[highestElement]->keyNode){
                BTreeNode *temp_node = rootNode->nextNode[highestElement]->nextNode[highestElement];
                tempNode->keyNode[1] = temp_node->keyNode[0];
            }

            if (rootNode != NULL)
            {
                tempNode->nextNode[0] = rootNode;
            }

            if (rootNode->nextNode[highestElement] != NULL)
            {
                tempNode->nextNode[1] = rootNode->nextNode[highestElement];
            }

            if (rootNode->nextNode[highestElement]->nextNode[highestElement] != NULL)
            {
                BTreeNode *temp_node = rootNode->nextNode[highestElement];
                tempNode->nextNode[2] = temp_node->nextNode[highestElement];
            }
        }
    }
    return RC_OK;
}


/**
 * 
 * This function scans the B-tree and sorts its elements in ascending order.
 * 
*/
RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle)
{
    idxNumber = 0;
    curr = (BTreeNode *)calloc(sizeof(BTreeNode), 1);
    curr = rootNode;

    BTreeNode *currNode;
    currNode = (BTreeNode *)malloc(sizeof(BTreeNode));
    int numOfElements, idx;
    numOfElements = 0;
    currNode = rootNode;
    while (currNode)
    {
        idx = 0;
        while (idx < highestElement)
        {
            if (currNode->keyNode[idx])
            {
                ++numOfElements;
            }
            ++idx;
        }
        currNode = currNode->nextNode[highestElement];
    }

    int keyNode[numOfElements], keyElements[highestElement][numOfElements], counterVar;
    counterVar = 0;
    currNode = rootNode;
    while (currNode)
    {
        idx = 0;
        while (idx < highestElement)
        {
            keyNode[counterVar] = currNode->keyNode[idx];
            keyElements[0][counterVar] = currNode->id[idx].page?currNode->id[idx].page:keyElements[0][counterVar];
            keyElements[1][counterVar] = currNode->id[idx].slot?currNode->id[idx].slot:keyElements[1][counterVar];
            printf("val1: %d  val2: %d", keyElements[0][counterVar], keyElements[1][counterVar]);
            counterVar++;
            idx++;
        }
        currNode = currNode->nextNode[highestElement];
    }

    int swapVar, temp_pgvar, var_st, x_var, y_var;
    x_var = 0;
    while (x_var < counterVar - 1)
    {
        y_var = 0;
        while (y_var < counterVar - x_var - 1)
        {

              if (keyNode[y_var] > keyNode[y_var + 1])
            {
                swapVar = keyNode[y_var];
                int ix;
                ix = 0;
                temp_pgvar = keyElements[ix++][y_var];
                var_st = keyElements[ix][y_var];

                if (y_var + 1 < sizeof(keyNode) / sizeof(int))
                {
                    keyNode[y_var] = keyNode[y_var + 1];
                    int val;
                    val = y_var+1;

                    keyElements[0][y_var] = keyElements[0][val];
                    keyElements[1][y_var] = keyElements[1][val];

                    if (y_var + 2 < sizeof(keyNode) / sizeof(int))
                    {
                        keyNode[y_var + 1] = swapVar;
                        if(temp_pgvar){
                            keyElements[0][y_var + 1] = temp_pgvar;
                        }
                        keyElements[1][y_var + 1] = var_st?var_st:keyElements[1][y_var + 1];
                    }
                }
            }
            y_var++;
        }
        x_var++;
    }

    counterVar = 0;
    currNode = rootNode;

    while (currNode)
    {
        idx = 0;
        while (idx < highestElement)
        {
            currNode->keyNode[idx] = keyNode[counterVar] ? keyNode[counterVar] : currNode->keyNode[idx];
            if (keyElements)
            {
                currNode->id[idx].page = keyElements[0][counterVar]?keyElements[0][counterVar]:currNode->id[idx].page;
                currNode->id[idx].slot = keyElements[1][counterVar]?keyElements[1][counterVar]:currNode->id[idx].slot;
            }
            counterVar++;
            idx++;
        }
        currNode = currNode->nextNode[highestElement];
    }
    return RC_OK;
}


/**
 * 
 * This function traverse through the BTree to find the node with the given key, and delete it if found
 * 
*/
RC deleteKey(BTreeHandle *tree, Value *keyNode)
{
    BTreeNode *currNode;
    currNode = (BTreeNode *)calloc(sizeof(BTreeNode), 1);
    int is_found, i, assign;
    is_found = 0;
    currNode = rootNode;

    assign = 0;

    while (currNode)
    {
        i = 0;
        while (i < highestElement)
        {
            if (currNode->keyNode[i] == keyNode->v.intV)
            {
                if (i < 0 || i >= MAX_KEYS_PER_NODE)
                {
                    // Handle invalid index
                }

                if (currNode->keyNode[i] != 0)
                {
                    currNode->keyNode[i] = currNode->keyNode[i]?0:currNode->keyNode[i];
                    currNode->id[i].page = currNode->id[i].slot = 0;
                    is_found = 1;
                    break;
                }
            }
            ++i;

            if (is_found)
            {
                break;
            }
        }
        currNode = currNode->nextNode[highestElement];
    }
    return RC_OK;
}


/**
 * 
 * This function returns the next entry in the B+ Tree index scan.It first checks if there is 
 * a next node, and then proceeds to update the result with the next entry's RID.
 * 
*/
RC nextEntry(BT_ScanHandle *handle, RID *result)
{
    if (curr->nextNode[highestElement])
    {
        if (highestElement == idxNumber)
        {
            if (curr->nextNode[highestElement] != NULL)
            {
                idxNumber = 0;
                curr = curr->nextNode[highestElement];
            }
            else
            {
                return RC_IM_NO_MORE_ENTRIES; 
            }
        }

        if (curr->id[idxNumber].page == 0 || curr->id[idxNumber].slot == 0)
        {
            return RC_IM_KEY_NOT_FOUND;
        }

        (*result).page = curr->id[idxNumber].page?curr->id[idxNumber].page: (*result).page;
        (*result).slot = curr->id[idxNumber].slot?curr->id[idxNumber].slot: (*result).slot;

        ++idxNumber;

        return RC_OK;
    }
    else
        return RC_IM_NO_MORE_ENTRIES;
}


/**
 * 
 * This function closes a scan on a B+ tree if it exists.
 * 
*/
RC closeTreeScan(BT_ScanHandle *handle)
{
    if (handle)
    {
        idxNumber = 0;
        return RC_OK;
    }
    return RC_MISC_ERROR;
}


/**
 * 
 * This function prints the keys of the B-tree.
 * 
*/
// debug and test functions
char *printTree(BTreeHandle *tree)
{
    BTreeNode *currNode = (BTreeNode *)malloc(sizeof(BTreeNode));
    int idx;
    currNode = rootNode;

    while (currNode != NULL)
    {
        idx = 0;
        while (idx < highestElement)
        {
            printf("%d\t", currNode->keyNode[idx]);
            idx++;
        }
        currNode = currNode->nextNode[highestElement];
    }
    return RC_OK;
}


/**
 * 
 * This function prints a message indicating that the index manager is shutting down
 * 
*/
RC shutdownIndexManager()
{
    printf("\n~~~~~~~~~~~~<INDEX MANAGER SHUTTING DOWN>~~~~~~~~~~~~");
    return RC_OK;
}
