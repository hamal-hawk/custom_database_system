compiler=gcc

x: dberror storage_mgr buffer_mgr_stat buffer_mgr test_assign2_1 link execute_testcase

dberror: dberror.c dberror.h 
	$(compiler) -c dberror.c

buffer_mgr_stat: buffer_mgr_stat.c buffer_mgr_stat.h
	$(compiler) -c buffer_mgr_stat.c

buffer_mgr: buffer_mgr.c buffer_mgr.h ds_define.h
	$(compiler) -c buffer_mgr.c

storage_mgr: storage_mgr.c storage_mgr.h
	$(compiler) -c storage_mgr.c

test_assign2_1: test_assign2_1.c test_helper.h
	$(compiler) -c test_assign2_1.c

link: test_assign2_1.o dberror.o buffer_mgr.o storage_mgr.o buffer_mgr_stat.o 
	$(compiler) -o  test_assign2 test_assign2_1.o dberror.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o

execute_testcase: test_assign2
	./test_assign2

clearall: test_assign2_1.o dberror.o storage_mgr.o
	rm -f  test_assign2 test_assign2_1.o dberror.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o 