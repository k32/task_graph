all:
	rebar3 do compile,dialyzer,eunit,ct
