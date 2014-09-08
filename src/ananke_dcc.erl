-module(ananke_dcc).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include("ananke_types.hrl").

%% @doc A Dotted Causal Container as described in "Concise Server-Wide Conflict
%% Management for Eventually Consistent Data Stores", by Goncalves, Almeida,
%% Baquero and Fonte. This paper is under embargo for now. A link will be
%% provided when made public.

%% API
-export([new/0,
         values/1,
         add/2,
         add/3,
         sync/2,
         discard/2,
         strip/2,
         fill/2]).

-export_type([dcc/0]).

-type bvv() :: ananke_bvv:bvv().

%% @doc A value and it's corresponding Dot
-type dotval() :: {dot(), value()}.
-type dotvals() :: orddict:orddict(dotval()).

%% @doc A Dotted Causal Container. This is a per-key type containing causality
%% information as well as sibling values.
-opaque dcc() :: {dotvals(), version_vector()}.

%% @doc Return an empty dcc
-spec new() -> dcc().
new() ->
    {orddict:new(), orddict:new()}.

%% @doc Return all the values in the dcc
-spec values(dcc()) -> [value()].
values({DotVals, _}) ->
    [Val || {_, Val} <- DotVals].

%% @doc Add the events corresponding to each dot in the DCC to the BVV
-spec add(bvv(), dcc()) -> bvv().
add(Bvv0, {DotVals, _}) ->
    orddict:fold(fun({Dot, _}, Bvv) ->
                     ananke_bvv:add(Bvv, Dot)
                 end, Bvv0, DotVals).

%% @doc Create a new entry in the DCC, and update its version vector
-spec add(dcc(), dot(), value()) -> dcc().
add({DotVals0, VV0}, {Actor, Event}=Dot, Val) ->
    DotVals = orddict:store(Dot, Val, DotVals0),
    VV = orddict:store(Actor, Event, VV0),
    {DotVals, VV}.

%% @doc Synchronize two DCCs. Discard versions made obsolete by the other DCC
%% using the version vectors of the DCCs.
-spec sync(dcc(), dcc()) -> dcc().
sync({D1, V1}, {D2, V2}) ->
    Matching = matching(D1, D2),
    N1 = discard_dotvals(D1, V2),
    N2 = discard_dotvals(D2, V1),
    DotVals = union(Matching, union(N1, N2)),
    VV = join(V1, V2),
    {DotVals, VV}.

%% @doc Discard obsolete dotvals and merge version vectors
%% VV2 is usually the client context in this function.
-spec discard(dcc(), version_vector()) -> dcc().
discard({DotVals0, VV1}, VV2) ->
    DotVals = discard_dotvals(DotVals0, VV2),
    VV = join(VV1, VV2),
    {DotVals, VV}.

%% @doc Return dotvals that aren't obsoleted by the version vector
-spec discard_dotvals(dotvals(), version_vector()) -> dotvals().
discard_dotvals(DV, VV) ->
    orddict:fold(
        fun({{Actor, Seq}=Dot, Val}, DotVals) ->
            case orddict:find(Actor, VV) of
                {ok, Seq2} when Seq > Seq2 ->
                    orddict:store(Dot, Val, DotVals);
                {ok, _} ->
                    DotVals;
                error ->
                    orddict:store(Dot, Val, DotVals)
            end
        end, orddict:new(), DV).

%% Remove all causality information covered by the node clock (BVV).
%% Specifically, remove all entries in the VV of the DCC that are covered by the
%% base of the BVV.
-spec strip(dcc(), bvv()) -> dcc().
strip({DV, VV0}, BVV) ->
    NewVV = orddict:fold(
                fun({Actor, Event}, VV) ->
                        Base = ananke_dvv:get_base(Actor, BVV),
                        case Event > Base of
                            true ->
                                orddict:store(Actor, Event, VV);
                            false ->
                                VV
                        end
                end, orddict:new(), VV0),
    {DV, NewVV}.

%% @doc Add causality information covered by the BVV back into the DCC.
%% Note that we wish to keep the type of the BVV opaque to this module.
%% However, both modules understand version_vectors(). Therefore we just use
%% a function in the bvv to do the heavy lifting.
-spec fill(dcc(), bvv()) -> dcc().
fill({DV, VV0}, BVV) ->
    VV = ananke_bvv:fill(VV0, BVV),
    {DV, VV}.

%% @doc Join two version vectors by taking the pointwise maximum.
-spec join(version_vector(), version_vector()) -> version_vector().
join(VV1, VV2) ->
    orddict:merge(fun(_, Val1, Val2) ->
                      max(Val1, Val2)
                  end, VV1, VV2).

%% @doc Return each dotval() that exist in both DV1 and DV2
-spec matching(dotvals(), dotvals()) -> dotvals().
matching(DV1, DV2) ->
    [X || X <- DV1, Y <- DV2, X =:= Y].

%% @doc Take the set union of the dotvals().
-spec union(dotvals(), dotvals()) -> dotvals().
union(DV1, DV2) ->
    %% Ensure the sort order is correct between orddicts and ordsets by using
    %% ordsets:from_list/1 and orddict:from_list/1. I'm not sure this is
    %% necessary and almost certainly adds overhead.
    O1 = ordsets:from_list(DV1),
    O2 = ordsets:from_list(DV2),
    O3 = ordsets:union(O1, O2),
    orddict:from_list(O3).
