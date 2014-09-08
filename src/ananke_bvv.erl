-module(ananke_bvv).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-endif.

-include_lib("ananke_types.hrl").

%% @doc A Bitmapped Version Vector as described in "Concise Server-Wide Conflict
%% Management for Eventually Consistent Data Stores", by Goncalves, Almeida,
%% Baquero and Fonte. This paper is under embargo for now. A link will be
%% provided when made public.

%% API
-export([new/0,
         add/2,
         event/2,
         join/2,
         base/1,
         get_base/2,
         fill/2]).

-export_type([bvv/0]).

%% @doc A downward-closed set of events.
%% The Base, N, represents the sequence of integers 0..N.
-type base() :: integer().

%% @doc All events occuring after the base, possibly with gaps.
%% The least significant bit is the first event after the base.
-type bitmap() :: integer().

%% @doc The bitmap version part of a bitmap version vector (bvv)
-type version() :: {base(), bitmap()}.

%% @doc A bitmapped version vector. This is the type of the node clock.
-opaque bvv() :: orddict:orddict({actor(), version()}).

%% @doc Return an empty bvv
-spec new() -> bvv().
new() ->
    orddict:new().

%% @doc Add a dot to the bvv
-spec add(bvv(), dot()) -> bvv().
add(Clock, {Actor, Event}) ->
    Version = get_version(Actor, Clock),
    NewVersion = update_version(Version, Event),
    orddict:store(Actor, NewVersion, Clock).

%% Return the base of the version for a given actor in a BVV
-spec get_base(actor(), bvv()) -> base().
get_base(Actor, Clock) ->
    {Base, _} = get_version(Actor, Clock),
    Base.

%% @doc Add causality information from the BVV into the VV
-spec fill(version_vector(), bvv()) -> version_vector().
fill(VV0, Clock) ->
    orddict:fold(fun({Actor, {Base, _}}, VV) ->
                     case orddict:find(Actor, VV0) of
                         {ok, Seq} ->
                             orddict:store(Actor, max(Seq, Base), VV);
                         error ->
                             orddict:store(Actor, Base, VV)
                     end
                 end, VV0, Clock).

%% @doc Takes a clock at a node and returns a pair with a sequence number for the
%% next event at that node, and a new clock with the event added.
%% Note that by definition, the clock representing this node has seen all events
%% for itself, and since its versions are always normalized, the next event is
%% simply Base + 1. Therefore you do not want to call this function with a Clock
%% that is not responsible for the given node, as it may not have the latest
%% information, and may crash because it's bitmap is not 0.
-spec event(bvv(), actor()) -> {event(), bvv()}.
event(Clock, Actor) ->
    {Base, 0} = get_version(Actor, Clock),
    Event = Base + 1,
    Version = {Event, 0},
    NewClock = orddict:store(Actor, Version, Clock),
    {Event, NewClock}.

%% @doc Add an event to the Version
%% In the refrenced paper, this is actually the first add function.
-spec update_version(version(), event()) -> version().
update_version({N, B}, M) when M =< N ->
    norm({N, B});
update_version({N, B}, M) ->
    B2 = B bor (1 bsl (M-1-N)),
    norm({N, B2}).

%% @doc Join events from clock C2 relevent to C1 into C1.
-spec join(bvv(), bvv()) -> bvv().
join(C1, C2) ->
    orddict:fold(
        fun({Actor, V1}, Clock) ->
            V2 = get_version(Actor, C2),
            NewVersion = join_versions(V1, V2),
            orddict:store(Actor, NewVersion, Clock)
        end, C1, C1).

%% @doc Take the union of the sets of events represented by the two versions.
%% In the referenced paper, this is actually the first join function.
-spec join_versions(version(), version()) -> version().
join_versions({N1, B1}, {N2, B2}) when N1 >= N2 ->
    B = B1 bor (B2 bsr (N1 - N2)),
    norm({N1, B});
join_versions({N1, B1}, {N2, B2}) ->
    B = B2 bor (B1 bsr (N2 - N1)),
    norm({N2, B}).

%% @doc Reset all the bitmaps to 0 in each clock entry's version
-spec base(bvv()) -> bvv().
base(Clock0) ->
    orddict:fold(fun({Actor, {Base, _}}, Clock) ->
                    NewVersion = {Base, 0},
                    orddict:store(Actor, NewVersion, Clock)
                 end, orddict:new(), Clock0).


%% @doc Normalize the bitmap to always be an even number. This minimizes
%% consecutive ones at the beginning of the bitmap and saves space.
-spec norm(version()) -> version().
norm({N, B}) when B rem 2 =/= 0 ->
    norm({N+1, B bsr 1});
norm(Vsn) ->
    Vsn.

-spec get_version(actor(), bvv()) -> version().
get_version(Actor, Clock) ->
    case orddict:find(Actor, Clock) of
        {ok, Version} ->
            Version;
        error ->
            fresh_version()
    end.

fresh_version() ->
    {0, 0}.

-ifdef(TEST).
%% TODO: move type counter, values/1 and values/3 outside this TEST conditional if/when used

-type counter() :: integer().

%% @doc List all sequence numbers for the given Version greater than N.
%% It's not particularly useful to list the consecutive values for 1..N, and
%% potentially a very dangerous consumer of resources. The bitmaps themselves
%% should over time contain much less values at any given time than the
%% total number of consecutive values less than N.
-spec values(version()) -> [pos_integer()].
values({N, B}) ->
    values(N+1, B, []).

%% @doc Check each successive bit in the bitmap, B, from least significant to
%% most significant bit, incrementing a counter, N, along the way. Accumulate N
%% in Events if the least significant Bit of the current bitmap is set.
-spec values(counter(), bitmap(), [event()]) -> [event()].
values(_N, 0, Events) ->
    lists:reverse(Events);
values(N, B, Events) when B rem 2 =/= 0 ->
    values(N+1, B bsr 1, [N | Events]);
values(N, B, Events) ->
    values(N+1, B bsr 1, Events).

simple_test() ->
    Clock = new(),
    Clock2 = add(Clock, {a, 1}),
    Version = get_version(a, Clock2),
    ?assertEqual({1, 0}, Version),
    ?assertEqual({1, 0}, norm({0,1})),
    ?assertEqual([], values({1, 0})),
    ?assertEqual([1], values({0, 1})),
    ?assertEqual([1,2,3], values({0, 7})),
    {Seq, Clock3} = event(Clock2, a),
    ?assertEqual(2, Seq),
    {Base, 0} = get_version(a, Clock3),
    ?assertEqual(2, Base).

-endif.
