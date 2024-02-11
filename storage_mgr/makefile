compiler=gcc

x: dberror storage_mgr test_assign1_1 link execute_testcase

dberror: dberror.c dberror.h 
	$(compiler) -c dberror.c

storage_mgr: storage_mgr.c storage_mgr.h
	$(compiler) -c storage_mgr.c

test_assign1_1: test_assign1_1.c test_helper.h
	$(compiler) -c test_assign1_1.c

link: test_assign1_1.o dberror.o storage_mgr.o
	$(compiler) -o  test_assign1 test_assign1_1.o dberror.o storage_mgr.o

execute_testcase: test_assign1
	./test_assign1

clearall: test_assign1_1.o dberror.o storage_mgr.o
	rm -f  test_assign1 test_assign1_1.o dberror.o storage_mgr.o