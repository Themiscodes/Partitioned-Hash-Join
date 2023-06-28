test_helpers_OBJS = test_helpers.o $(LIB)/phjlib.a
test_hopscotch_OBJS = test_hopscotch.o $(LIB)/phjlib.a
test_partition_OBJS = test_partition.o $(LIB)/phjlib.a
test_phjoin_OBJS = test_phjoin.o $(LIB)/phjlib.a
test_query_OBJS = test_query.o $(LIB)/phjlib.a
test_relation_OBJS = test_relation.o $(LIB)/phjlib.a
test_optimizer_OBJS = test_optimizer.o $(LIB)/phjlib.a

include ../common.mk

.PHONY: $(LIB)/phjlib.a
$(LIB)/phjlib.a:
	$(MAKE) -C $(LIB) phjlib.a
