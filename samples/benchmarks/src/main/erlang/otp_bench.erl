#!/usr/bin/env escript
%% -*- erlang -*-
%%
%% otp_bench.erl — Erlang/OTP competitive benchmarks
%%
%% Measures the same scenarios as the kotlin-otp InvestigationBenchmarks.kt:
%%   1. gen_server:call roundtrip (sequential, single caller)
%%   2. gen_server:cast enqueue throughput
%%   3. supervisor single restart (warm)
%%   4. gen_server:call concurrent callers (N=1,10,50,100)
%%
%% Output format: CSV rows matching CompetitorBenchmarks.kt column order:
%%   library,scenario,iterations,p50_us,p95_us,p99_us,p999_us,throughput_ops_sec,notes
%%
%% Usage:
%%   escript otp_bench.erl [--profile=quick|long] [--rounds=N]

-module(otp_bench).
-mode(compile).

%% ---------------------------------------------------------------------------
%% Echo gen_server
%% ---------------------------------------------------------------------------

-behaviour(gen_server).
-export([start_link/0, stop/1,
         init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

start_link() ->
    gen_server:start_link(?MODULE, [], []).

stop(Pid) ->
    gen_server:stop(Pid).

init([]) ->
    {ok, #{cast_count => 0}}.

handle_call(ping, _From, State) ->
    {reply, pong, State};
handle_call(count, _From, State = #{cast_count := N}) ->
    {reply, N, State};
handle_call(_, _From, State) ->
    {reply, ok, State}.

handle_cast(cast, State = #{cast_count := N}) ->
    {noreply, State#{cast_count := N + 1}};
handle_cast(_, State) ->
    {noreply, State}.

handle_info(_, State) -> {noreply, State}.
terminate(_, _) -> ok.

%% ---------------------------------------------------------------------------
%% Statistics helpers
%% ---------------------------------------------------------------------------

percentile(Sorted, Pct) ->
    N = length(Sorted),
    Idx = max(1, round(Pct / 100.0 * N)),
    Clamped = min(Idx, N),
    lists:nth(Clamped, Sorted).

%% Convert nanoseconds list to microseconds float
nanos_to_us(Ns) -> Ns / 1000.0.

summarise(NanosList) ->
    Sorted = lists:sort(NanosList),
    N = length(Sorted),
    TotalNs = lists:sum(Sorted),
    P50  = nanos_to_us(percentile(Sorted, 50.0)),
    P95  = nanos_to_us(percentile(Sorted, 95.0)),
    P99  = nanos_to_us(percentile(Sorted, 99.0)),
    P999 = nanos_to_us(percentile(Sorted, 99.9)),
    ThroughputOpsPerSec = N * 1.0e9 / TotalNs,
    {N, P50, P95, P99, P999, ThroughputOpsPerSec}.

%% ---------------------------------------------------------------------------
%% Call roundtrip benchmark
%% ---------------------------------------------------------------------------

bench_call_roundtrip(Iterations, WarmupIterations) ->
    {ok, Pid} = start_link(),

    %% Warm up
    lists:foreach(fun(_) -> gen_server:call(Pid, ping) end,
                  lists:seq(1, WarmupIterations)),

    %% Measure
    Latencies = lists:map(
        fun(_) ->
            T0 = erlang:monotonic_time(nanosecond),
            pong = gen_server:call(Pid, ping),
            T1 = erlang:monotonic_time(nanosecond),
            T1 - T0
        end,
        lists:seq(1, Iterations)),

    stop(Pid),
    {Iterations, Latencies}.

%% ---------------------------------------------------------------------------
%% Cast enqueue benchmark
%% ---------------------------------------------------------------------------

bench_cast_enqueue(Iterations, WarmupIterations) ->
    {ok, Pid} = start_link(),

    lists:foreach(fun(_) -> gen_server:cast(Pid, cast) end,
                  lists:seq(1, WarmupIterations)),

    T0 = erlang:monotonic_time(nanosecond),
    Latencies = lists:map(
        fun(_) ->
            S = erlang:monotonic_time(nanosecond),
            gen_server:cast(Pid, cast),
            E = erlang:monotonic_time(nanosecond),
            E - S
        end,
        lists:seq(1, Iterations)),
    T1 = erlang:monotonic_time(nanosecond),

    %% Drain: confirm all casts processed
    _Count = gen_server:call(Pid, count),
    stop(Pid),

    TotalNs = T1 - T0,
    {Iterations, Latencies, TotalNs}.

%% ---------------------------------------------------------------------------
%% Supervisor restart benchmark
%% ---------------------------------------------------------------------------

bench_supervisor_restart(WarmupRestarts) ->
    %% Warm-up phase: crash child WarmupRestarts times, discard timing
    lists:foreach(
        fun(I) ->
            Self = self(),
            Sup = simple_supervisor(1, Self),
            receive stable -> ok after 5000 -> error({warmup_timeout, I}) end,
            exit(Sup, shutdown)
        end,
        lists:seq(1, WarmupRestarts)),

    %% Measurement: 1 crash → stable
    Self = self(),
    T0 = erlang:monotonic_time(nanosecond),
    Sup = simple_supervisor(1, Self),
    receive stable -> ok after 5000 -> error(measure_timeout) end,
    T1 = erlang:monotonic_time(nanosecond),
    exit(Sup, shutdown),

    T1 - T0.

%% Spawn a one_for_one supervisor whose single child crashes CrashesLeft times
%% before completing.  Uses proc_lib + gen_server directly so we avoid needing
%% a supervision module at compile time.
simple_supervisor(CrashesLeft, NotifyPid) ->
    Self = self(),
    SupPid = spawn_link(fun() -> run_simple_supervisor(CrashesLeft, NotifyPid, Self) end),
    receive {sup_ready, SupPid} -> ok after 5000 -> error(sup_start_timeout) end,
    SupPid.

run_simple_supervisor(CrashesLeft, NotifyPid, Parent) ->
    process_flag(trap_exit, true),
    Parent ! {sup_ready, self()},
    start_crashable_child(CrashesLeft, NotifyPid),
    supervisor_loop(CrashesLeft, NotifyPid).

supervisor_loop(CrashesLeft, NotifyPid) ->
    receive
        {'EXIT', _Child, normal} ->
            ok;
        {'EXIT', _Child, _Reason} ->
            %% Restart
            NewCrashesLeft = CrashesLeft - 1,
            start_crashable_child(NewCrashesLeft, NotifyPid),
            supervisor_loop(NewCrashesLeft, NotifyPid);
        {shutdown} ->
            ok
    end.

start_crashable_child(CrashesLeft, NotifyPid) ->
    Self = self(),
    spawn_link(fun() -> crashable_child(CrashesLeft, NotifyPid, Self) end).

crashable_child(CrashesLeft, NotifyPid, _SupPid) ->
    case CrashesLeft > 0 of
        true  -> error({intentional_crash, CrashesLeft});
        false -> NotifyPid ! stable, receive shutdown -> ok end
    end.

%% ---------------------------------------------------------------------------
%% Concurrent callers benchmark
%% ---------------------------------------------------------------------------

bench_concurrent_callers(Iterations, WarmupIterations, CallerCount) ->
    {ok, Pid} = start_link(),

    %% Warm up single-caller
    lists:foreach(fun(_) -> gen_server:call(Pid, ping) end,
                  lists:seq(1, WarmupIterations)),

    Self = self(),
    Barrier = make_ref(),

    %% Spawn N callers; each waits for the barrier, then runs CallsPerCaller calls
    CallsPerCaller = Iterations,
    Pids = lists:map(
        fun(_) ->
            spawn(fun() ->
                receive {go, Barrier} -> ok end,
                Lats = lists:map(
                    fun(_) ->
                        T0 = erlang:monotonic_time(nanosecond),
                        pong = gen_server:call(Pid, ping),
                        T1 = erlang:monotonic_time(nanosecond),
                        T1 - T0
                    end,
                    lists:seq(1, CallsPerCaller)),
                Self ! {done, self(), Lats}
            end)
        end,
        lists:seq(1, CallerCount)),

    T0 = erlang:monotonic_time(nanosecond),
    %% Release all callers simultaneously
    lists:foreach(fun(P) -> P ! {go, Barrier} end, Pids),
    %% Collect results
    AllLatencies = lists:foldl(
        fun(_, Acc) ->
            receive {done, _, Lats} -> Lats ++ Acc end
        end,
        [],
        Pids),
    T1 = erlang:monotonic_time(nanosecond),

    stop(Pid),
    TotalNs = T1 - T0,
    {length(AllLatencies), AllLatencies, TotalNs}.

%% ---------------------------------------------------------------------------
%% CSV output
%% ---------------------------------------------------------------------------

csv_row(Library, Scenario, Iterations, P50, P95, P99, P999, Throughput, Notes) ->
    io:format("~s,~s,~w,~.2f,~.2f,~.2f,~.2f,~.2f,~s~n",
              [Library, Scenario, Iterations, P50, P95, P99, P999, Throughput, Notes]).

%% ---------------------------------------------------------------------------
%% Main
%% ---------------------------------------------------------------------------

main(Args) ->
    %% Suppress BEAM crash/error reports from intentional crashes in supervisor
    %% restart warm-up. They are expected and would pollute the CSV output.
    error_logger:tty(false),
    Profile = parse_profile(Args),
    #{iterations := Iterations,
      warmup     := Warmup,
      callers    := Callers,
      rounds     := Rounds} = Profile,

    io:format("profile=~s,rounds=~w,iterations=~w~n",
              [maps:get(name, Profile), Rounds, Iterations]),
    io:format("library,scenario,iterations,p50_us,p95_us,p99_us,p999_us,throughput_ops_sec,notes~n"),

    lists:foreach(
        fun(Round) ->
            run_round(Round, Iterations, Warmup, Callers),
            io:format(standard_error, "round=~w complete~n", [Round])
        end,
        lists:seq(1, Rounds)).

run_round(Round, Iterations, Warmup, CallerCounts) ->
    %% 1. Call roundtrip
    {N1, Lats1} = bench_call_roundtrip(Iterations, Warmup),
    {N1, P50_1, P95_1, P99_1, P999_1, Tp1} = summarise(Lats1),
    csv_row("otp", "call_roundtrip", N1, P50_1, P95_1, P99_1, P999_1, Tp1,
            "gen_server:call; same-node; single sequential caller"),

    %% 2. Cast enqueue
    {N2, Lats2, TotalNs2} = bench_cast_enqueue(Iterations, Warmup),
    {N2, P50_2, P95_2, P99_2, P999_2, _} = summarise(Lats2),
    Tp2 = N2 * 1.0e9 / TotalNs2,
    csv_row("otp", "cast_enqueue", N2, P50_2, P95_2, P99_2, P999_2, Tp2,
            "gen_server:cast; enqueue cost only; drain confirmed via count call"),

    %% 3. Supervisor restart
    WarmupRestarts = 10,
    RestartNs = bench_supervisor_restart(WarmupRestarts),
    RestartUs = nanos_to_us(RestartNs),
    RestartTp = 1.0e9 / RestartNs,
    csv_row("otp", "supervisor_single_restart", 1,
            RestartUs, RestartUs, RestartUs, RestartUs, RestartTp,
            "one_for_one; 1 crash then stable; " ++
            integer_to_list(WarmupRestarts) ++ " JIT warm-up restarts"),

    %% 4. Concurrent callers
    lists:foreach(
        fun(CallerCount) ->
            CallsPerCaller = max(1, Iterations div max(1, CallerCount)),
            {Nc, Latsc, TotalNsc} = bench_concurrent_callers(CallsPerCaller, Warmup, CallerCount),
            {Nc, P50c, P95c, P99c, P999c, _} = summarise(Latsc),
            Tpc = Nc * 1.0e9 / TotalNsc,
            Scenario = "concurrent_callers_" ++ integer_to_list(CallerCount),
            Notes = integer_to_list(CallerCount) ++
                    " concurrent Erlang processes; CountDownLatch-equiv barrier",
            csv_row("otp", Scenario, Nc, P50c, P95c, P99c, P999c, Tpc, Notes)
        end,
        CallerCounts),

    Round.  %% satisfy foreach — the value is discarded

%% ---------------------------------------------------------------------------
%% Profile parsing
%% ---------------------------------------------------------------------------

parse_profile(Args) ->
    ProfileName = case lists:keyfind("--profile=long", 1,
                                      [{A, A} || A <- Args]) of
        {_, _} -> "long";
        false  ->
            case lists:member("--profile=long", Args) of
                true  -> "long";
                false -> "quick"
            end
    end,
    Rounds = case [R || "--rounds=" ++ R <- Args] of
        [S | _] ->
            case string:to_integer(S) of
                {N, _} when N > 0 -> N;
                _                 -> default_rounds(ProfileName)
            end;
        [] -> default_rounds(ProfileName)
    end,
    case ProfileName of
        "long" ->
            #{name       => "long",
              iterations => 50000,
              warmup     => 5000,
              callers    => [1, 10, 50, 100],
              rounds     => Rounds};
        _ ->
            #{name       => "quick",
              iterations => 20000,
              warmup     => 2000,
              callers    => [1, 10, 50],
              rounds     => Rounds}
    end.

default_rounds("long")  -> 5;
default_rounds(_)       -> 1.
