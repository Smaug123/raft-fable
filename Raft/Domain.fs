namespace Raft

/// LogIndex is indexed from 1. We use 0 to indicate "before any history has started".
[<Measure>]
type LogIndex

[<Measure>]
type Term

[<Measure>]
type ServerId

[<Measure>]
type ClientId

[<Measure>]
type ClientSequence

type LogEntryMetadata =
    {
        Index : int<LogIndex>
        Term : int<Term>
    }

    override this.ToString () =
        sprintf "Log entry %i at subjective term %i" this.Index this.Term

type ClientResponse =
    | NotLeader of leaderHint : int<ServerId> option
    | SessionExpired
    | Success of int<ClientId> * int<ClientSequence>

    override this.ToString () =
        match this with
        | ClientResponse.NotLeader hint ->
            let hint =
                match hint with
                | None -> ""
                | Some leader -> sprintf " (leader hint: %i)" leader

            sprintf "Failed to send data due to not asking leader%s" hint
        | ClientResponse.SessionExpired -> "Failed to send data as session expired"
        | ClientResponse.Success (client, sequence) -> sprintf "Client %i's request %i succeeded" client sequence

type RegisterClientResponse =
    | NotLeader of leaderHint : int<ServerId> option
    | Success of int<ClientId>

    override this.ToString () =
        match this with
        | RegisterClientResponse.Success client -> sprintf "Registered client with ID %i" client
        | RegisterClientResponse.NotLeader hint ->
            let hint =
                match hint with
                | None -> ""
                | Some leader -> sprintf " (leader hint: %i)" leader

            sprintf "Failed to register client due to not asking leader%s" hint

type InternalRaftCommunication = | NewClientRegistered of (RegisterClientResponse -> unit)

type LogEntry<'a> =
    | ClientEntry of 'a * int<ClientId> * int<ClientSequence> * (ClientResponse -> unit)
    | RaftOverhead of InternalRaftCommunication

    override this.ToString () =
        match this with
        | LogEntry.ClientEntry (data, client, sequence, _) ->
            sprintf "Client %i, sequence number %i, sends data %O" client sequence data
        | LogEntry.RaftOverhead (InternalRaftCommunication.NewClientRegistered _) -> "New client registration"
