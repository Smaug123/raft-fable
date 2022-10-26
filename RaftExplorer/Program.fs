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

    type UserAction =
        | Timeout of int<ServerId>
        | NetworkMessage of int<ServerId> * int

    let rec getAction (clusterSize : int) =
        printf "Enter action. Trigger [t]imeout <server id>, or allow [m]essage <server id, message id>: "
        let s = Console.ReadLine().ToUpperInvariant ()

        match s.[0] with
        | 'T' ->
            match getTimeout clusterSize s.[1..] with
            | Some t -> t |> Timeout
            | None -> getAction clusterSize
        | 'M' ->
            match getMessage clusterSize s.[1..] with
            | Some m -> m |> NetworkMessage
            | None -> getAction clusterSize
        | _ ->
            printf "Unrecognised input. "
            getAction clusterSize

    [<EntryPoint>]
    let main _argv =
        let clusterSize = 5
        let cluster, network = InMemoryCluster.make<int> clusterSize

        while true do
            printNetworkState network

            let action = getAction clusterSize

            match action with
            | Timeout serverId -> cluster.Timeout serverId
            | NetworkMessage (serverId, messageId) ->
                network.InboundMessage serverId messageId |> cluster.SendMessage serverId
                network.DropMessage serverId messageId

        0
