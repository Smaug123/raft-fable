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

    let electLeader =
        [
            NetworkAction.InactivityTimeout 0<ServerId>
            NetworkAction.NetworkMessage (1<ServerId>, 0)
            NetworkAction.NetworkMessage (2<ServerId>, 0)
            NetworkAction.DropMessage (3<ServerId>, 0)
            NetworkAction.DropMessage (4<ServerId>, 0)
            NetworkAction.NetworkMessage (0<ServerId>, 0)
            NetworkAction.NetworkMessage (0<ServerId>, 1)
        // At this point, server 0 is leader in an uncontested election.
        ]

    [<EntryPoint>]
    let main _argv =
        let clusterSize = 5
        let cluster, network = InMemoryCluster.make<int> clusterSize

        let startupSequence =
            [
                NetworkAction.InactivityTimeout 0<ServerId>
                NetworkAction.InactivityTimeout 1<ServerId>
                // Two servers vote for server 1...
                NetworkAction.NetworkMessage (2<ServerId>, 1)
                NetworkAction.NetworkMessage (3<ServerId>, 1)
                // One server votes for server 0...
                NetworkAction.NetworkMessage (4<ServerId>, 0)
                // and the other votes are processed and discarded
                NetworkAction.NetworkMessage (0<ServerId>, 0)
                NetworkAction.NetworkMessage (1<ServerId>, 0)
                NetworkAction.NetworkMessage (2<ServerId>, 0)
                NetworkAction.NetworkMessage (3<ServerId>, 0)
                NetworkAction.NetworkMessage (4<ServerId>, 1)
                // Server 0 process incoming votes
                NetworkAction.NetworkMessage (0<ServerId>, 1)
                // Server 1 processes incoming votes, and achieves majority, electing itself leader!
                NetworkAction.NetworkMessage (1<ServerId>, 1)
                NetworkAction.NetworkMessage (1<ServerId>, 2)
                // Get the followers' heartbeat processing out of the way
                NetworkAction.NetworkMessage (2<ServerId>, 2)
                NetworkAction.NetworkMessage (3<ServerId>, 2)
                NetworkAction.NetworkMessage (4<ServerId>, 2)
                NetworkAction.NetworkMessage (1<ServerId>, 3)
                NetworkAction.NetworkMessage (1<ServerId>, 4)
                NetworkAction.NetworkMessage (1<ServerId>, 5)
                // Server 0 processes the leader's heartbeat and drops out of the election.
                NetworkAction.NetworkMessage (0<ServerId>, 2)
                NetworkAction.NetworkMessage (1<ServerId>, 6)
            ]

        for action in startupSequence do
            NetworkAction.perform cluster network action

        while true do
            printNetworkState network
            printClusterState cluster

            let action = getAction clusterSize
            NetworkAction.perform cluster network action

        // TODO: log out the committed state so that we can see whether the leader is correctly
        // processing heartbeat responses
        // TODO: allow client queries!

        0
