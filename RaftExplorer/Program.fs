namespace Raft.Explorer

open System
open Raft

module Program =

    let printNetworkState<'a> (network : Network<'a>) : unit =
        let mutable wroteAnything = false

        for i in 0 .. network.Size - 1 do
            for count, message in Seq.indexed (network.AllInboundMessages (i * 1<ServerId>)) do
                printfn "Server %i, message %i: %O" i count message
                wroteAnything <- true

        if not wroteAnything then
            printfn "<No messages in network>"

    let rec getMessage (clusterSize : int) =
        printf "Enter <server ID, message ID>: "
        let s = Console.ReadLine ()

        match s.Split ',' with
        | [| serverId ; messageId |] ->
            match Int32.TryParse serverId with
            | true, serverId ->
                match Int32.TryParse messageId with
                | true, messageId ->
                    if serverId >= clusterSize || serverId < 0 then
                        printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
                        getMessage clusterSize
                    else
                        serverId * 1<ServerId>, messageId
                | false, _ ->
                    printf "Non-integer input '%s' for message ID. " messageId
                    getMessage clusterSize
            | false, _ ->
                printf "Non-integer input '%s' for server ID. " serverId
                getMessage clusterSize
        | _ ->
            printfn "Invalid input."
            getMessage clusterSize

    let rec getTimeout (clusterSize : int) =
        printf "Enter server ID: "
        let serverId = Console.ReadLine ()

        match Int32.TryParse serverId with
        | true, serverId ->
            if serverId >= clusterSize || serverId < 0 then
                printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
                getTimeout clusterSize
            else
                serverId * 1<ServerId>
        | false, _ ->
            printf "Unrecognised input. "
            getTimeout clusterSize

    type UserAction =
        | Timeout of int<ServerId>
        | NetworkMessage of int<ServerId> * int

    let rec getAction (clusterSize : int) =
        printf "Enter action. Trigger [t]imeout, or allow [m]essage: "
        let s = Console.ReadLine().ToUpperInvariant ()

        match s with
        | "T" -> getTimeout clusterSize |> Timeout
        | "M" -> getMessage clusterSize |> NetworkMessage
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

        0
