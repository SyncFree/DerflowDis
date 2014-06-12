-module(b_test).
-export([test/0, test1/0, test2/0, bindVar/2, bindValue/2, producer/3, buffer/3, consumer/3]).

test() ->
    {id, S1}=derflowdis:declare(),
    {id, S2}=derflowdis:declare(),
    {id, S3}=derflowdis:declare(),
    {id, S4}=derflowdis:declare(),
    derflowdis:thread(b_test,bindVar, [S1, S2]),
    derflowdis:thread(b_test,bindValue, [S2, 100]),
    derflowdis:thread(b_test,bindValue, [S4, 20]),
    derflowdis:thread(b_test,bindVar, [S3, S4]).


test1() ->
    {id, S1}=derflowdis:declare(),
    {id, S2}=derflowdis:declare(),
    {id, S3}=derflowdis:declare(),
    derflowdis:thread(b_test,bindVar, [S2, S3]),
    derflowdis:thread(b_test,bindVar, [S1, S3]),
    derflowdis:thread(b_test,bindValue, [S3, 100]).

test2() ->
    {id, S1}=derflowdis:declare(),
    {id, S2}=derflowdis:declare(),
    {id, S3}=derflowdis:declare(),
    derflowdis:thread(b_test,bindVar, [S2, S3]),
    derflowdis:thread(b_test,bindVar, [S1, S2]),
    derflowdis:thread(b_test,bindValue, [S3, 100]).

bindVar(X1, Y1) ->
   Next = derflowdis_vnode:bind(X1, {id,Y1}),
   lager:info("Bind finished ~w ~w Next ~w ~n", [X1, Y1,Next]),
   X2=derflowdis_vnode:read(X1),
   %Y2=derflowdis_vnode:read(Y1),
   lager:info("Value of ~w: ~w~n", [X1,X2]).


bindValue(Y1, V) ->
   receive
   after 100 -> lager:info("~n")
   end,
   _X = derflowdis_vnode:bind(Y1, V),
   lager:info("Bind Value finished ~w ~w ~n",[Y1,V]).

producer(Value, N, Output) ->
    if (N>0) ->
        derflowdis:waitNeeded(Output),
	{id,Next} = derflowdis:bind(Output, Value),
        producer(Value+1, N-1,  Next);
    true ->
        derflowdis:bind(Output, nil)
    end.

loop(S1, S2, End) ->
    derflowdis:waitNeeded(S2),
    {S1Value, S1Next} = derflowdis:read(S1),
    {id, S2Next} = derflowdis:bind(S2, S1Value),
    {PS1, _} = S1,
    {PS2, _} = S2,
    lager:info("Buff:Bound for consumer ~w-> ~w ~w~n",[PS1,PS2,S1Value]),
    case derflowdis:next(End) of {nil, _} ->
        ok;	
	EndNext ->
       loop(S1Next, S2Next, EndNext)    
    end.

buffer(S1, Size, S2) ->
    End = drop_list(S1, Size),
    lager:info("Buff:End of list ~w ~n",[End]),
    loop(S1, S2, End).

drop_list(S, Size) ->
    if Size == 0 ->
	S;
      true ->
       	Next = derflowdis:next(S),
	lager:info("Drop next ~w ~n",[S]),
    	drop_list(Next, Size-1)
    end.

consumer(S2, Size, F) ->
    if Size == 0 ->
	lager:info("Finished~n");
	true ->
	    case derflowdis:read(S2) of
		{nil, _} ->
	   	lager:info("Cons:Reading end~n");
		{Value, Next} ->
	   	{PS2,_} = S2,
	   	lager:info("Cons:Id ~w Consume ~w, Get ~w, Next~w ~n",[PS2,Value, F(Value),Next]),
	   	consumer(Next, Size-1, F)
    	end
    end.


