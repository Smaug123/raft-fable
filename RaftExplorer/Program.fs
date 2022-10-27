namespace Raft.Explorer

open System
open Raft

module Program =

    let printNetworkState<'a> (network : Network<'a>) : unit =
        let mutable wroteAnything = false

        for i in 0 .. network.ClusterSize - 1 do
            for messageId, message in network.UndeliveredMessages (i * 1<ServerId>) do
                printfn "Server %i, message %i: %O" i messageId message
                wroteAnything <- true

        if not wroteAnything then
            printfn "<No messages in network>"

    let printClusterState<'a> (cluster : Cluster<'a>) : unit =
        for i in 0 .. cluster.ClusterSize - 1 do
            printfn "Server %i: %O" i (cluster.State (i * 1<ServerId>))

    let getMessage (clusterSize : int) (s : string) : (int<ServerId> * int) option =
        match s.Split ',' with
        | [| serverId ; messageId |] ->
            let serverId = serverId.Trim ()
            let messageId = messageId.Trim ()

            match Int32.TryParse serverId with
            | true, serverId ->
                match Int32.TryParse messageId with
                | true, messageId ->
                    if serverId >= clusterSize || serverId < 0 then
                        printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
                        None
                    else
                        Some (serverId * 1<ServerId>, messageId)
                | false, _ ->
                    printf "Non-integer input '%s' for message ID. " messageId
                    None
            | false, _ ->
                printf "Non-integer input '%s' for server ID. " serverId
                None
        | _ ->
            printfn "Invalid input."
            None

    let rec getTimeout (clusterSize : int) (serverId : string) =
        match Int32.TryParse serverId with
        | true, serverId ->
            if serverId >= clusterSize || serverId < 0 then
                printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
                None
            else
                Some (serverId * 1<ServerId>)
        | false, _ ->
            printf "Unrecognised input. "
            None

    let rec getHeartbeater (clusterSize : int) (serverId : string) =
        // TODO: restrict this to the leaders only
        match Int32.TryParse serverId with
        | true, serverId ->
            if serverId >= clusterSize || serverId < 0 then
                printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
                None
            else
                Some (serverId * 1<ServerId>)
        | false, _ ->
            printf "Unrecognised input. "
            None

    type UserAction =
        | InactivityTimeout of int<ServerId>
        | NetworkMessage of int<ServerId> * int
        | DropMessage of int<ServerId> * int
        | Heartbeat of int<ServerId>

    let rec getAction (clusterSize : int) =
        printf
            "Enter action. Trigger [t]imeout <server id>, [h]eartbeat a leader <server id>, [d]rop message <server id, message id>, or allow [m]essage <server id, message id>: "

        let s =
            let rec go () =
                let s = Console.ReadLine().ToUpperInvariant ()
                if String.IsNullOrEmpty s then go () else s

            go ()

        match s.[0] with
        | 'T' ->
            match getTimeout clusterSize s.[1..] with
            | Some t -> t |> InactivityTimeout
            | None -> getAction clusterSize
        | 'D' ->
            match getMessage clusterSize s.[1..] with
            | Some m -> m |> DropMessage
            | None -> getAction clusterSize
        | 'M' ->
            match getMessage clusterSize s.[1..] with
            | Some m -> m |> NetworkMessage
            | None -> getAction clusterSize
        | 'H' ->
            match getHeartbeater clusterSize s.[1..] with
            | Some h -> Heartbeat h
            | None -> getAction clusterSize
        | _ ->
            printf "Unrecognised input. "
            getAction clusterSize

    let processAction (cluster : Cluster<'a>) (network : Network<'a>) (action : UserAction) : unit =
        match action with
        | InactivityTimeout serverId -> cluster.InactivityTimeout serverId
        | Heartbeat serverId -> cluster.HeartbeatTimeout serverId
        | DropMessage (serverId, messageId) -> network.DropMessage serverId messageId
        | NetworkMessage (serverId, messageId) ->
            network.InboundMessage serverId messageId |> cluster.SendMessage serverId
            network.DropMessage serverId messageId


    [<EntryPoint>]
    let main _argv =
        let clusterSize = 5
        let cluster, network = InMemoryCluster.make<int> clusterSize

        let startupSequence =
            [
                UserAction.InactivityTimeout 0<ServerId>
                UserAction.NetworkMessage (1<ServerId>, 0)
                UserAction.NetworkMessage (2<ServerId>, 0)
                UserAction.DropMessage (3<ServerId>, 0)
                UserAction.DropMessage (4<ServerId>, 0)
                UserAction.NetworkMessage (0<ServerId>, 0)
                UserAction.NetworkMessage (0<ServerId>, 1)
            ]

        for action in startupSequence do
            processAction cluster network action

        while true do
            printNetworkState network
            printClusterState cluster

            let action = getAction clusterSize
            processAction cluster network action

        // TODO: log out the committed state so that we can see whether the leader is correctly
        // processing heartbeat responses
        // TODO: allow client queries!

        0
