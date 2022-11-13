namespace Raft

open System
open System.Collections.Generic

type Cluster<'a> =
    internal
        {
            Servers : Server<'a> array
            SendMessageDirectly : int<ServerId> -> Message<'a> -> unit
        }

    member this.SendMessage (i : int<ServerId>) (m : Message<'a>) : unit = this.SendMessageDirectly i m

    member this.InactivityTimeout (i : int<ServerId>) : unit =
        this.Servers[ i / 1<ServerId> ].TriggerInactivityTimeout ()
        this.Servers[ i / 1<ServerId> ].Sync ()

    member this.HeartbeatTimeout (i : int<ServerId>) : unit =
        this.Servers[ i / 1<ServerId> ].TriggerHeartbeatTimeout ()
        this.Servers[ i / 1<ServerId> ].Sync ()

    member this.Status (i : int<ServerId>) : ServerStatus = this.Servers[i / 1<ServerId>].State

    member this.GetCurrentInternalState (i : int<ServerId>) : ServerInternalState<'a> Async =
        this.Servers[ i / 1<ServerId> ].GetCurrentInternalState ()

    member this.ClusterSize : int = this.Servers.Length

    member this.Leaders : Set<int<ServerId>> =
        ((Set.empty, 0<ServerId>), this.Servers)
        ||> Array.fold (fun (leaders, count) server ->
            let leaders =
                match server.State with
                | ServerStatus.Leader _ -> Set.add count leaders
                | _ -> leaders

            leaders, count + 1<ServerId>
        )
        |> fst

type Network<'a> =
    internal
        {
            /// CompleteMessageHistory.[i] is the collection of all messages
            /// ever sent to server `i`.
            CompleteMessageHistory : ResizeArray<Message<'a>>[]
            MessagesDelivered : HashSet<int>[]
        }

    static member Make (clusterSize : int) =
        {
            CompleteMessageHistory = Array.init clusterSize (fun _ -> ResizeArray ())
            MessagesDelivered = Array.init clusterSize (fun _ -> HashSet ())
        }

    member this.AllInboundMessages (i : int<ServerId>) : Message<'a> list =
        this.CompleteMessageHistory[i / 1<ServerId>] |> List.ofSeq

    member this.InboundMessage (i : int<ServerId>) (id : int) : Message<'a> =
        this.CompleteMessageHistory[i / 1<ServerId>].[id]

    member this.DropMessage (i : int<ServerId>) (id : int) : unit =
        this.MessagesDelivered[ i / 1<ServerId> ].Add id |> ignore

    member this.UndeliveredMessages (i : int<ServerId>) : (int * Message<'a>) list =
        this.CompleteMessageHistory[i / 1<ServerId>]
        |> Seq.indexed
        |> Seq.filter (fun (count, _) -> this.MessagesDelivered[ i / 1<ServerId> ].Contains count |> not)
        |> List.ofSeq

    member this.AllUndeliveredMessages () : (int * Message<'a>) list list =
        List.init this.CompleteMessageHistory.Length (fun i -> this.UndeliveredMessages (i * 1<ServerId>))

    member this.ClusterSize = this.CompleteMessageHistory.Length

[<RequireQualifiedAccess>]
module InMemoryCluster =

    [<RequiresExplicitTypeArguments>]
    let make<'a> (count : int) : Cluster<'a> * Network<'a> =
        let servers = Array.zeroCreate<Server<'a>> count

        let network = Network<int>.Make count

        let messageChannelHold (serverId : int<ServerId>) (message : Message<'a>) : unit =
            let arr = network.CompleteMessageHistory[serverId / 1<ServerId>]
            lock arr (fun () -> arr.Add message)

        for s in 0 .. servers.Length - 1 do
            servers[s] <- Server (count, s * 1<ServerId>, InMemoryPersistentState (), messageChannelHold)

        let cluster =
            {
                Servers = servers
                SendMessageDirectly =
                    fun i m ->
                        servers[ i / 1<ServerId> ].Message m
                        servers[ i / 1<ServerId> ].Sync ()
            }

        cluster, network

type NetworkAction<'a> =
    | InactivityTimeout of int<ServerId>
    | NetworkMessage of int<ServerId> * int
    | DropMessage of int<ServerId> * int
    | ClientRequest of int<ServerId> * ClientRequest<'a>
    | Heartbeat of int<ServerId>

[<RequireQualifiedAccess>]
module NetworkAction =

    let perform<'a> (cluster : Cluster<'a>) (network : Network<'a>) (action : NetworkAction<'a>) : unit =
        match action with
        | InactivityTimeout serverId -> cluster.InactivityTimeout serverId
        | Heartbeat serverId -> cluster.HeartbeatTimeout serverId
        | DropMessage (serverId, messageId) -> network.DropMessage serverId messageId
        | NetworkMessage (serverId, messageId) ->
            network.InboundMessage serverId messageId |> cluster.SendMessage serverId
            network.DropMessage serverId messageId
        | ClientRequest (server, request) -> Message.ClientRequest request |> cluster.SendMessage server

    let private getMessage (clusterSize : int) (s : EfficientString) : Result<int<ServerId> * int, string> =
        let mutable messageId = s
        let serverId = EfficientString.takeUntil ',' &messageId

        let serverId = serverId.TrimEnd ()
        let messageId = messageId.Trim ()

        match Int32.TryParse serverId with
        | false, _ -> sprintf "Non-integer input '%s' for server ID." (serverId.ToString ()) |> Error
        | true, serverId ->

        match Int32.TryParse messageId with
        | false, _ ->
            sprintf "Non-integer input '%s' for message ID." (messageId.ToString ())
            |> Error

        | true, messageId ->

        if serverId >= clusterSize || serverId < 0 then
            sprintf "Server ID must be between 0 and %i inclusive, got %i" (clusterSize - 1) serverId
            |> Error
        else
            Ok (serverId * 1<ServerId>, messageId)

    let private getTimeout (clusterSize : int) (serverId : EfficientString) : Result<int<ServerId>, string> =
        match Int32.TryParse serverId with
        | false, _ -> Error (sprintf "Expected an integer, got '%s'" (serverId.ToString ()))
        | true, serverId ->

        if serverId >= clusterSize || serverId < 0 then
            sprintf "Server ID must be between 0 and %i inclusive, got %i." (clusterSize - 1) serverId
            |> Error
        else
            serverId * 1<ServerId> |> Ok

    let private getHeartbeat (leaders : Set<int<ServerId>> option) (clusterSize : int) (serverId : EfficientString) =
        match Int32.TryParse serverId with
        | false, _ ->
            sprintf "Expected an integer server ID, got '%s'" (serverId.ToString ())
            |> Error
        | true, serverId ->

        if serverId >= clusterSize || serverId < 0 then
            sprintf "Server ID must be between 0 and %i inclusive, got %i." (clusterSize - 1) serverId
            |> Error
        else

        let serverId = serverId * 1<ServerId>

        match leaders with
        | None -> Ok serverId
        | Some leaders ->

        if leaders |> Set.contains serverId then
            Ok serverId
        else
            sprintf "Cannot heartbeat a non-leader (%i)." serverId |> Error

    let private getNewClientTarget<'a>
        (clusterSize : int)
        (serverId : EfficientString)
        : Result<int<ServerId>, string>
        =
        match Int32.TryParse serverId with
        | false, _ ->
            sprintf "Expected an int for a server ID, got '%s'" (serverId.ToString ())
            |> Error
        | true, serverId ->

        if serverId >= clusterSize || serverId < 0 then
            sprintf "Server ID must be between 0 and %i inclusive, got %i." (clusterSize - 1) serverId
            |> Error
        else
            Ok (serverId * 1<ServerId>)

    /// Mutates the input byref to contain the result.
    let private getClientSubmitData<'a>
        (clusterSize : int)
        (s : byref<EfficientString>)
        : Result<int<ServerId> * int<ClientId> * int<ClientSequence>, string>
        =
        let serverId = EfficientString.takeUntil ',' &s
        let clientId = EfficientString.takeUntil ',' &s
        let clientSequenceNumber = EfficientString.takeUntil ',' &s

        match Int32.TryParse (serverId.Trim ()) with
        | false, _ ->
            sprintf "Server ID expected as first comma-separated component, got '%s'." (serverId.ToString ())
            |> Error
        | true, serverId ->

        if serverId >= clusterSize || serverId < 0 then
            sprintf "Server ID must be between 0 and %i inclusive, got %i." (clusterSize - 1) serverId
            |> Error
        else

        match Int32.TryParse (clientId.Trim ()) with
        | false, _ ->
            sprintf "Client ID expected as second comma-separated component, got '%s'." (clientId.ToString ())
            |> Error
        | true, clientId ->

        match Int32.TryParse (clientSequenceNumber.Trim ()) with
        | false, _ ->
            sprintf
                "Client sequence number expected as third comma-separated component, got '%s'."
                (clientSequenceNumber.ToString ())
            |> Error
        | true, clientSequenceNumber ->

        (serverId * 1<ServerId>, clientId * 1<ClientId>, clientSequenceNumber * 1<ClientSequence>)
        |> Ok

    /// Optionally also validates leaders against the input set of leaders.
    let tryParse<'a>
        (parse : string -> Result<'a, string>)
        (leaders : Set<int<ServerId>> option)
        (handleRegisterClientResponse : RegisterClientResponse -> unit)
        (handleClientDataResponse : ClientResponse -> unit)
        (clusterSize : int)
        (s : string)
        : Result<NetworkAction<'a>, string>
        =
        if String.IsNullOrEmpty s then
            Error "Can't parse an empty string"
        else

        let rest = EfficientString.slice 1 (s.Length - 1) (EfficientString.ofString s)

        match Char.ToUpper s[0] with
        | 'T' ->
            match getTimeout clusterSize (EfficientString.trimStart rest) with
            | Ok t -> t |> InactivityTimeout |> Ok
            | Error e -> Error e
        | 'D' ->
            match getMessage clusterSize (EfficientString.trimStart rest) with
            | Ok m -> m |> DropMessage |> Ok
            | Error e -> Error e
        | 'M' ->
            match getMessage clusterSize (EfficientString.trimStart rest) with
            | Ok m -> m |> NetworkMessage |> Ok
            | Error e -> Error e
        | 'H' ->
            match getHeartbeat leaders clusterSize (EfficientString.trimStart rest) with
            | Ok h -> Heartbeat h |> Ok
            | Error e -> Error e
        | 'S' ->
            match getNewClientTarget clusterSize (EfficientString.trimStart rest) with
            | Ok target ->
                ClientRequest (target, ClientRequest.RegisterClient handleRegisterClientResponse)
                |> Ok
            | Error e -> Error e
        | 'R' ->
            let mutable rest = EfficientString.trimStart rest

            match getClientSubmitData clusterSize &rest with
            | Ok (server, client, sequence) ->
                match parse (rest.ToString ()) with
                | Ok data ->
                    (server, ClientRequest.ClientRequest (client, sequence, data, handleClientDataResponse))
                    |> ClientRequest
                    |> Ok
                | Error e -> Error e
            | Error e -> Error e
        | c -> Error (sprintf "unexpected start char '%c'" c)

    let toString<'a> (action : NetworkAction<'a>) : string =
        match action with
        | NetworkAction.Heartbeat h -> sprintf "h %i" h
        | NetworkAction.NetworkMessage (server, id) -> sprintf "m %i,%i" server id
        | NetworkAction.DropMessage (server, id) -> sprintf "d %i,%i" server id
        | NetworkAction.InactivityTimeout server -> sprintf "t %i" server
        | NetworkAction.ClientRequest (server, ClientRequest.RegisterClient _) -> sprintf "s %i" server
        | NetworkAction.ClientRequest (server, ClientRequest.ClientRequest (client, sequence, data, _)) ->
            sprintf "r %i,%i,%i,%O" server client sequence data
