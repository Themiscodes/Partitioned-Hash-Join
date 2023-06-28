MAKE += --silent

# All directories inside the "programs" directory
PROGRAMS = $(subst programs/, , $(wildcard programs/*))

all: lib programs tests

ci-checks: check-lint run-tests clean

prepare: lint run-tests clean

lib:
	$(MAKE) -C lib all

# Create a target programs-<foo> for each <foo> (same logic for other rules below)
programs-%:
	$(MAKE) -C programs/$*

# Depend on programs-<foo> for each item in PROGRAMS (same logic for other rules below)
programs: $(addprefix programs-, $(PROGRAMS))

lint:
	find . -name '*.c' -o -name '*.h' ! -name 'acutest.h' | xargs clang-format -i -style=file

check-lint:
	find . -name '*.c' -o -name '*.h' ! -name 'acutest.h' | xargs clang-format --dry-run -Werror -style=file

tests:
	$(MAKE) -C tests all

run: run-tests run-programs

run-programs-%:
	$(MAKE) -C programs/$* run

run-programs: $(addprefix run-programs-, $(PROGRAMS))

run-harness:
	$(MAKE) -C programs/sigmod run-harness

run-tests:
	$(MAKE) -C tests run

valgrind: valgrind-tests valgrind-programs

valgrind-programs-%:
	$(MAKE) -C programs/$* valgrind

valgrind-programs: $(addprefix valgrind-programs-, $(PROGRAMS))

valgrind-tests:
	$(MAKE) -C tests valgrind
	$(MAKE) clean

clean-%:
	$(MAKE) -C programs/$* clean

clean: $(addprefix clean-, $(PROGRAMS))
	$(MAKE) -C tests clean
	$(MAKE) -C lib clean

.PHONY: all ci-checks prepare lib programs lint check-lint tests run run-programs \
        run-tests valgrind valgrind-programs valgrind-tests clean
