.PHONY: all build concuerror

CONCUERROR_BIN:=Concuerror/_build/default/bin/concuerror

all: build # concuerror_tests

build:
	rebar3 do compile,dialyzer,eunit,ct,cover

# Concuerror doesn't support ets:take :(
concuerror_tests: $(CONCUERROR_BIN)
	ERL_LIBS=./_build/test/lib $(CONCUERROR_BIN) --file test/concuerror_tests.erl -t gc_test

$(CONCUERROR_BIN):
	git clone https://github.com/parapluu/Concuerror.git && \
	cd Concuerror && \
	make

coveralls:
	rebar3 do cover -v, coveralls send
