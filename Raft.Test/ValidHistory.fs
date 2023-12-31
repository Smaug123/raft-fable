namespace Raft.Test

open Raft
open FsCheck

type ValidHistory<'a> = | ValidHistory of NetworkAction<'a> list

[<RequireQualifiedAccess>]
module ValidHistory =
    let validate<'a> (clusterSize : int) (history : NetworkAction<'a> list) : ValidHistory<'a> option =
        let cluster, network = InMemoryCluster.make<'a> clusterSize

        let mutable isValid = true

        try
            for action in history do
                NetworkAction.perform cluster network action
        with _ ->
            isValid <- false

        if isValid then Some (ValidHistory history) else None

    let private historyGenOfLength<'a>
        (elementGen : Gen<'a>)
        (clusterSize : int)
        (len : int)
        : Gen<NetworkAction<'a> list>
        =
        let cluster, network = InMemoryCluster.make<'a> clusterSize
        // Note: takes a reversed list.
        let permissibleNext () : NetworkAction<'a> list =
            let state = network.CompleteMessageHistory

            [
                for i in 0 .. clusterSize - 1 do
                    let server = i * 1<ServerId>

                    for messageId in 0 .. state.[i].Count - 1 do
                        yield NetworkAction.DropMessage (server, messageId)
                        yield NetworkAction.NetworkMessage (server, messageId)

                    yield NetworkAction.Heartbeat server
                    yield NetworkAction.InactivityTimeout server
            ]

        (*
        let clientRequestGen =
            gen {
                let! element = elementGen
                let! id = Gen.choose (0, clusterSize - 1)
                return NetworkAction.ClientRequest (id * 1<ServerId>, element)
            }
        *)

        let rec go (len : int) =
            gen {
                if len = 0 then
                    return []
                else
                    let! smaller = go (len - 1)

                    let! next = Gen.elements (permissibleNext ())
                    (*
                        clientRequestGen :: List.replicate 5 (Gen.elements (permissibleNext ()))
                        |> Gen.oneof
                        *)

                    NetworkAction.perform cluster network next
                    return next :: smaller
            }

        go (abs len)

    let gen<'a> (elementGen : Gen<'a>) (clusterSize : int) : Gen<ValidHistory<'a>> =
        historyGenOfLength<'a> elementGen clusterSize
        |> Gen.sized
        |> Gen.map (List.rev >> ValidHistory)

    let shrink<'a> (clusterSize : int) (ValidHistory history : ValidHistory<'a>) =
        let removeOne =
            Seq.init history.Length (fun i -> List.removeAt i history)
            |> Seq.choose (validate clusterSize)

        let shrinkMessageId =
            history
            |> Seq.indexed
            |> Seq.choose (fun (i, action) ->
                let newMessage =
                    match action with
                    | NetworkAction.DropMessage (server, i) ->
                        if i > 0 then
                            Some (NetworkAction.DropMessage (server, i - 1))
                        else
                            None
                    | NetworkAction.NetworkMessage (server, i) ->
                        if i > 0 then
                            Some (NetworkAction.NetworkMessage (server, i - 1))
                        else
                            None
                    | _ -> None

                newMessage
                |> Option.map (fun message -> history |> List.removeAt i |> List.insertAt i message)
            )
            |> Seq.choose (validate clusterSize)

        Seq.concat [ removeOne ; shrinkMessageId ]

    let arb<'a> (elementGen : Gen<'a>) (clusterSize : int) =
        { new Arbitrary<ValidHistory<'a>>() with
            override _.Generator = gen elementGen clusterSize

            override _.Shrinker history = shrink clusterSize history
        }
