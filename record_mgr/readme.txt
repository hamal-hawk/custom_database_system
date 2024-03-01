CS525: Advanced Database Organization
Assignment 3 - Record Manager
Group 25


Team Members
Rushikesh Kadam A20517258
Haren Amal A20513547
Gabriel Baranes A20521263


How to run the program:
1. Go to the project root folder (assign2) from the terminal.
2. Execute "make clearall" command to clear all the previously compiled files (.o files).
3. Execute "make" command to compile the program files, link and create executables, and then run the test cases.



Functions Overview:

// table and manager

a- initRecordManager:
	This function is used to initialize the record manager. It calls the function 	initStorageManager()
	
b- shutdownRecordManager:
	This function is Used to dhut down the record manager. It also frees the 	memory allocated to the 'ph'

c- createTable:
	This function is used to create a new table

d- openTable:
	This function opens a table with a given name. It initializes a buffer pool, 	pins the first page of the table, reads the schema from the page, and unpins 	the page.
	

e- closeTable:
	This function writes the table metadata to the first page of the buffer pool 	and shuts it down. It constructs a string containing the table's metadata, 	pins the first page, writes the metadata, marks it as dirty, and unpins it. 	Finally, it shuts down the buffer pool and returns RC_OK.

f- deleteTable:
	This function deletes a table with the given name and shuts down the buffer 	pool associated with it. It first checks that the table name is not null, 	then deletes the page file for the table. 

g- getNumTuples:
	This function returns the total number of records in the table referenced by 	the input pointer, stored in the table management information structure.

// handling records in a table

h- insertRecord:
	The function inserts a record into a table by locating a free page and slot, 	writing the record data into the page at the corresponding offset, and 	updating the bookkeeping information for the table. The function returns an 	RC code indicating the success or failure of the operation.

i- deleteRecord:
	This  function deletes a record from a table using the page number and slot 	number provided in the RID parameter. The record is first located in the 	buffer pool, and then its values are set to null using the memset() function.

j- updateRecord:
	This function updates a record in a table by modifying the data of the record 	with the given ID. It searches for the record in the table using the page 	number and slot number of the record, and updates the record data by copying 	the new data into the record's position in the page.

k- getRecord: 
	This function retrieves a record with a given ID (RID) from a buffer pool 	page, copies its content into the Record structure, and returns RC_OK.

// scans

l- startScan:
	This function initializes a new scan on the given table using the specified 	condition, and sets the scan handle's management data to point to a struct 	containing information about the scan. 

m- next:
	This function reads the next record in a scan, evaluates the condition 	for the record, and returns the record if the condition is true. If the end 	of the table is reached or there are no more records that match the 	condition, it returns an error code.

n- closeScan:
	This function resets the scan information to its initial state and returns 	RC_OK.


// dealing with schemas


o- getRecordSize:
	This function takes a schema pointer as input, iterates through the schema's 	attributes, and calculates the size of a record based on the data types and 	type lengths of the attributes. It returns the calculated size.

p- createSchema:
	This function dynamically allocates memory for a Schema struct and 	initializes its fields, including the attribute names, data types, type 	lengths, key size, and key attributes. It also sets the size of a record 	based on the schema and initializes the total number of records in the table 	to 0.

q- freeSchema:
	This function frees the memory allocated for a schema


// dealing with records and attribute values


r- createRecord:
	This function creates a new record with the given schema, allocates memory 	for its data, sets the page ID to -1, and returns it through a pointer. If 	memory allocation fails, it returns an error message.

s- freeRecord:
	This function frees the memory allocated to a Record struct, including its 	data buffer. It also sets the Record pointer to NULL to avoid any dangling 	references.

t- getAttr:
	This function retrieves the value of a given attribute in a record by parsing 	the record's binary data according to the schema, and returns the value in 	the form of a Value data type. 

u- setAttr:
	This function sets the attribute value of a record in the specified position 	based on its attribute number, as defined in the provided schema. The 	attribute value is given by a provided value, which can be of different data 	types.

