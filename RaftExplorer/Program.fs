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
            printfn "Server %i: %O" i (cluster.Status (i * 1<ServerId>))

    let rec getAction (leaders : Set<int<ServerId>>) (clusterSize : int) : NetworkAction<byte> =
        printf
            "Enter action. Trigger [t]imeout <server id>, [h]eartbeat a leader <server id>, [d]rop message <server id, message id>, establish new [s]ession <server id>, or allow [m]essage <server id, message id>: "

        let s =
            let rec go () =
                let s = Console.ReadLine().ToUpperInvariant ()
                if String.IsNullOrEmpty s then go () else s

            go ()

        let parseByte (s : string) =
            match Byte.TryParse s with
            | true, b -> Ok b
            | false, _ -> Error (sprintf "expected a byte, got '%s'" s)

        let handleRegister (response : RegisterClientResponse) : unit =
            match response with
            | RegisterClientResponse.Success i -> printfn "Client successfully registered, getting ID %i" i
            | RegisterClientResponse.NotLeader hint ->
                match hint with
                | Some hint -> printfn "Client failed to register due to not asking a leader; try asking server %i" hint
                | None -> printfn "Client failed to register due to not asking a leader."

        let handleResponse (response : ClientResponse) : unit =
            match response with
            | ClientResponse.NotLeader hint ->
                match hint with
                | Some hint ->
                    printfn "Client failed to send request due to not asking a leader; try asking server %i" hint
                | None -> printfn "Client failed to send request due to not asking a leader."
            | ClientResponse.SessionExpired ->
                failwith "Client failed to send request due to expiry of session. This currently can't happen."
            | ClientResponse.Success (client, sequence) ->
                printfn "Raft has committed request from client %i with sequence number %i" client sequence

        match NetworkAction.tryParseString parseByte (Some leaders) handleRegister handleResponse clusterSize s with
        | Ok action -> action
        | Error e ->
            printfn "%s" e
            getAction leaders clusterSize

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
        let cluster, network = InMemoryCluster.make<byte> clusterSize

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
            |> ignore

            []

        for action in startupSequence do
            NetworkAction.perform cluster network action

        while true do
            printNetworkState network
            printClusterState cluster

            let leaders =
                Seq.init
                    clusterSize
                    (fun i ->
                        let i = i * 1<ServerId>
                        i, cluster.Status i
                    )
                |> Seq.choose (fun (i, status) ->
                    match status with
                    | Leader _ -> Some i
                    | _ -> None
                )
                |> Set.ofSeq

            let action = getAction leaders clusterSize
            NetworkAction.perform cluster network action

        // TODO: log out the committed state so that we can see whether the leader is correctly
        // processing heartbeat responses
        0
