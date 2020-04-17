%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(coap_dtls_listen_sup).

-author("dwg <dwg@emqx.io>").

-behaviour(supervisor).

%% API
-export([start_link/2]).

%% Supervisor callbacks
-export([init/1]).

%%%===================================================================
%%% API functions
%%%===================================================================

%%--------------------------------------------------------------------
%% @doc
%% Starts the supervisor
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(InPort, Opts0) ->
    Opts =  Opts0 ++ [binary, {protocol, dtls}, {reuseaddr, true}],
    {ok, Sup} = supervisor:start_link(?MODULE, []),
    {ok, SocketSup} = supervisor:start_child(Sup,
        {socket_sup, {coap_dtls_socket_sup, start_link, []},
         transient, infinity, supervisor, [coap_dtls_socket_sup]}),
    {ok, _Listener}  = supervisor:start_child(Sup,
        {listener, {coap_dtls_listen, start_link, [SocketSup, InPort, Opts]},
         transient, 16#ffffffff, worker, [coap_dtls_listen]}),
    {ok, Sup}.

init([]) ->
    {ok, {{rest_for_one, 10, 3600}, []}}.
