.PHONY: all build concuerror

ROOT=$(shell pwd)
CONCUERROR_BIN=$(ROOT)/Concuerror/_build/default/bin/concuerror

all: build

build:
	rebar3 do compile,dialyzer,eunit,ct

# TODO: This is exteremely messy, mostly for internal use
concuerror_tests: $(CONCUERROR_BIN)
	rebar3 as concuerror eunit && \
	ERL_LIBS=$(ROOT)/_build/concuerror/lib \
	$(CONCUERROR_BIN) -a 1000 -pz ./_build/concuerror+test/lib/task_graph/test/ --file test/concuerror_tests.erl -t gc_test

$(CONCUERROR_BIN):
	git clone https://github.com/parapluu/Concuerror.git && \
	cd Concuerror && \
	make
