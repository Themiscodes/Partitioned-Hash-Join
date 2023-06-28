# For each Makefile, add <foo>_OBJS for each target executable <foo> with all
# object file dependencies and include this file.
#
# Example: myprogram_OBJS = file1.o file2.o
#
# The common.mk file is used for creating the targets:
#
# - all             depends on all targets <foo>
# - run             depends on all targets run-<foo>
# - valgrind        depends on all targets valgrind-<foo>
# - <foo>           compiles <foo>
# - run-<foo>       compiles & executes <foo>
# - valgrind-<foo>  compiles & executes <foo> in valgrind

# Paths
MY_PATH := $(dir $(lastword $(MAKEFILE_LIST)))
MODULES := $(MY_PATH)modules
INCLUDE := $(MY_PATH)include
LIB     := $(MY_PATH)lib

# Use the gcc compiler with the following flags set:
#
# - Use the C11 standard
# - Activate level 2 optimizations
# - Treat all warnings as errors & enable extra warnings
# - Disable any strict aliasing assumptions for extra safety
# - Look for header files under the "include" directory
#
# Note: "override" lets us pass more parameters from the command line (make CFLAGS=...)

CC = gcc

override CFLAGS += -std=c11 -O2 -Wall -Wextra -Werror -fno-strict-aliasing -I$(INCLUDE) 
override LDFLAGS += -lpthread -lm

# List of all executables & libraries <foo> for which there's a <foo>_OBJS variable
WITH_OBJS := $(subst _OBJS,,$(filter %_OBJS,$(.VARIABLES)))
PROGS := $(filter-out %.a,$(WITH_OBJS))
LIBS := $(filter %.a,$(WITH_OBJS))

# Get all objects here
OBJS := $(foreach target, $(WITH_OBJS), $($(target)_OBJS))

# List of all run-<foo>, valgrind-<foo> targets (?= means "assign if variable is unassigned")
RUN_TARGETS ?= $(addprefix run-, $(PROGS))
VAL_TARGETS ?= $(addprefix valgrind-, $(PROGS))

# For each test, set its parameters from default to --time
$(foreach test, $(filter test_%, $(PROGS)), $(eval $(test)_ARGS ?= --time))


# Make it so that this rule can be overrided / extended in other makefiles
all:: $(PROGS)

# This is needed so that we can use variables in dependency lists, which requires "$$"
.SECONDEXPANSION:

# For each executable, create a target that declares its list of dependencies 
$(PROGS): $$($$@_OBJS)
	$(CC) $^ -o $@ $(LDFLAGS)

# For each library <lib>, create a target that declares its list of dependencies
$(LIBS): $$($$@_OBJS)
	ar -rcs $@ $^

# For each executable <foo>, create a target run-<foo> that executes it with parameters <foo>_ARGS
run-%: %
	./$* $($*_ARGS)

run: $(RUN_TARGETS)

# For each executable <foo>, create a target valgrind-<foo> that executes it in valgrind with parameters <foo>_ARGS
valgrind-%: %
	valgrind --error-exitcode=1 --leak-check=full --show-leak-kinds=all --track-origins=yes ./$* $($*_ARGS)

valgrind: $(VAL_TARGETS)

clean::
	@$(RM) $(PROGS) $(LIBS) $(OBJS)

.PHONY: all run valgrind clean
