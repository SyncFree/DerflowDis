%% @doc Interface for riak_searchng-admin commands.
-module(derflowdis_console).
-export([staged_join/1,
         down/1,
         ringready/1]).
-ignore_xref([join/1,
              leave/1,
              remove/1,
              ringready/1]).

staged_join([NodeStr]) ->
    Node = list_to_atom(NodeStr),
    join(NodeStr, fun riak_core:staged_join/1,
         "Success: staged join request for ~p to ~p~n", [node(), Node]).

join(NodeStr, JoinFn, SuccessFmt, SuccessArgs) ->
    try
        case JoinFn(NodeStr) of
            ok ->
                lager:info(SuccessFmt, SuccessArgs),
                ok;
            {error, not_reachable} ->
                lager:info("Node ~s is not reachable!~n", [NodeStr]),
                error;
            {error, different_ring_sizes} ->
                lager:info("Failed: ~s has a different ring_creation_size~n",
                          [NodeStr]),
                error;
            {error, unable_to_get_join_ring} ->
                lager:info("Failed: Unable to get ring from ~s~n", [NodeStr]),
                error;
            {error, not_single_node} ->
                lager:info("Failed: This node is already a member of a "
                          "cluster~n"),
                error;
            {error, self_join} ->
                lager:info("Failed: This node cannot join itself in a "
                          "cluster~n"),
                error;
            {error, _} ->
                lager:info("Join failed. Try again in a few moments.~n", []),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Join failed ~p:~p", [Exception, Reason]),
            lager:info("Join failed, see log for details~n"),
            error
    end.


down([Node]) ->
    try
        case riak_core:down(list_to_atom(Node)) of
            ok ->
                lager:info("Success: ~p marked as down~n", [Node]),
                ok;
            {error, legacy_mode} ->
                lager:info("Cluster is currently in legacy mode~n"),
                ok;
            {error, is_up} ->
                lager:info("Failed: ~s is up~n", [Node]),
                error;
            {error, not_member} ->
                lager:info("Failed: ~p is not a member of the cluster.~n",
                          [Node]),
                error;
            {error, only_member} ->
                lager:info("Failed: ~p is the only member.~n", [Node]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Down failed ~p:~p", [Exception, Reason]),
            lager:info("Down failed, see log for details~n"),
            error
    end.

ringready([]) ->
    try
        case riak_core_status:ringready() of
            {ok, Nodes} ->
                lager:info("TRUE All nodes agree on the ring ~p\n", [Nodes]);
            {error, {different_owners, N1, N2}} ->
                lager:info("FALSE Node ~p and ~p list different partition owners\n", [N1, N2]),
                error;
            {error, {nodes_down, Down}} ->
                lager:info("FALSE ~p down.  All nodes need to be up to check.\n", [Down]),
                error
        end
    catch
        Exception:Reason ->
            lager:error("Ringready failed ~p:~p", [Exception,
                    Reason]),
            lager:info("Ringready failed, see log for details~n"),
            error
    end.
