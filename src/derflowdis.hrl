-define(PRINT(Var), lager:info("DEBUG: ~p:~p - ~p~n~n ~p~n~n", [?MODULE, ?LINE, ??Var, Var])).

-define(BUCKET, <<"derflow">>).
