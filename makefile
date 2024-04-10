test_assign3_1:
	gcc -o test_assign3_1.o test_assign3_1.c rm_serializer.c expr.c record_mgr.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c hash_table.c

test_assign3_2:
	gcc -o test_assign3_2.o test_assign3_2.c rm_serializer.c expr.c record_mgr.c buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c hash_table.c


.PHONY: clean
clean:
	rm -f test_assign3_1.o
	rm -f test_assign3_2.o
	rm -f DATA.bin