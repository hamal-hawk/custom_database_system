compiler=gcc -std=c99

x: dberror btree_mgr record_mgr buffer_mgr_stat buffer_mgr storage_mgr expr rm_serializer test_assign4_1 link execute_testcase

dberror: dberror.c dberror.h 
	$(compiler) -c dberror.c

btree_mgr: btree_mgr.c btree_mgr.h
	$(compiler) -c btree_mgr.c

record_mgr: record_mgr.c record_mgr.h
	$(compiler) -c storage_mgr.c

buffer_mgr_stat: buffer_mgr_stat.c buffer_mgr_stat.h
	$(compiler) -c buffer_mgr_stat.c

buffer_mgr: buffer_mgr.c buffer_mgr.h ds_define.h
	$(compiler) -c buffer_mgr.c

storage_mgr: storage_mgr.c storage_mgr.h
	$(compiler) -c storage_mgr.c

expr: expr.c expr.h 
	$(compiler) -c expr.c

rm_serializer: rm_serializer.c tables.h
	$(compiler) -c rm_serializer.c

test_assign4_1: test_assign4_1.c test_helper.h
	$(compiler) -c test_assign4_1.c

link: test_assign4_1.o dberror.o  btree_mgr.o record_mgr.o buffer_mgr.o storage_mgr.o buffer_mgr_stat.o expr.o rm_serializer.o
	$(compiler) -o  test_assign4 test_assign4_1.o dberror.o btree_mgr.o record_mgr.o buffer_mgr.o storage_mgr.o buffer_mgr_stat.o expr.o rm_serializer.o

execute_testcase: test_assign4
	./test_assign4

clearall: test_assign4_1.o dberror.o storage_mgr.o
	rm -f  test_assign4 test_assign4_1.o dberror.o btree_mgr.o record_mgr.o buffer_mgr.o buffer_mgr_stat.o storage_mgr.o rm_serializer.o expr.o