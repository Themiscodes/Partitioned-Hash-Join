joiner_OBJS = joiner.o $(LIB)/phjlib.a

include ../../common.mk

all:: harness

$(LIB)/phjlib.a:
	$(MAKE) -C $(LIB) phjlib.a

harness:
	g++ -O2 -std=c++14 harness.cpp -o harness -lpthread

run-harness: harness joiner
	./harness public_alt/public.init public_alt/public.work public_alt/public.result ./joiner

run-public: harness joiner
	./harness public/public.init public/public.work public/public.result ./joiner

run-small: harness joiner
	./harness workloads/small.init workloads/small.work workloads/small.result ./joiner

clean::
	@$(RM) harness

.PHONY: $(LIB)/phjlib.a harness run-harness
