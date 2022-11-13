namespace Raft.Test

open System.Threading
open Raft
open NUnit.Framework
open FsUnitTyped
open FsCheck

[<TestFixture>]
module TestInMemoryServer =

    let check<'T> (prop : 'T) =
        let config =
            { Config.QuickThrowOnFailure with
                MaxTest = 1000
            }

        Check.One (config, prop)

    let parseByte (s : string) : Result<byte, string> =
        match System.Byte.TryParse s with
        | false, _ -> Error (sprintf "oh no: %s" s)
        | true, v -> Ok v

    [<Test>]
    let ``Can round-trip NetworkAction`` () =
        let property (action : NetworkAction<byte>) =
            let roundTripped =
                NetworkAction.toString action
                |> NetworkAction.tryParse parseByte None ignore ignore 5
                |> Result.get

            match roundTripped, action with
            | NetworkAction.ClientRequest (server1, request1), NetworkAction.ClientRequest (server2, request2) ->
                match request1, request2 with
                | ClientRequest.ClientRequest (client1, seq1, data1, _),
                  ClientRequest.ClientRequest (client2, seq2, data2, _) ->
                    server1 = server2 && client1 = client2 && seq1 = seq2 && data1 = data2
                | ClientRequest.RegisterClient _, ClientRequest.RegisterClient _ -> server1 = server2
                | _, _ -> false
            | NetworkAction.InactivityTimeout server1, NetworkAction.InactivityTimeout server2 -> server1 = server2
            | NetworkAction.Heartbeat server1, NetworkAction.Heartbeat server2 -> server1 = server2
            | NetworkAction.DropMessage (server1, message1), NetworkAction.DropMessage (server2, message2) ->
                server1 = server2 && message1 = message2
            | NetworkAction.NetworkMessage (server1, message1), NetworkAction.NetworkMessage (server2, message2) ->
                server1 = server2 && message1 = message2
            | _, _ -> false

        property |> Prop.forAll (NetworkAction.generate 5 |> Arb.fromGen) |> check

    [<Test>]
    let ``Startup sequence in prod, only one timeout takes place`` () =
        let cluster, network = InMemoryCluster.make<int> 5

        NetworkAction.InactivityTimeout 0<ServerId>
        |> NetworkAction.perform cluster network

        // We sent a message to every other server; process them.
        for i in 1..4 do
            let server = i * 1<ServerId>

            (network.AllInboundMessages server).Length |> shouldEqual 1

            NetworkAction.NetworkMessage (server, 0)
            |> NetworkAction.perform cluster network

            (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual i

        for i in 1..4 do
            NetworkAction.NetworkMessage (0<ServerId>, (i - 1))
            |> NetworkAction.perform cluster network

        // (the messages we've already processed)
        (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual 4

        (network.UndeliveredMessages 0<ServerId>).Length |> shouldEqual 0

        cluster.Servers.[0].State |> shouldEqual (ServerStatus.Leader 1<Term>)

        for i in 1..4 do
            cluster.Servers.[i].State |> shouldEqual ServerStatus.Follower

    let popOne (queues : 'a list list) : ((int * 'a) * 'a list list) list =
        queues
        |> List.indexed
        |> List.filter (fun (index, l) -> not (List.isEmpty l))
        |> List.collect (fun (firstPlaceWithInstruction, entries) ->
            entries
            |> List.indexed
            |> List.map (fun (i, entry) -> (firstPlaceWithInstruction, entry), List.removeAt i entries)
            |> List.map (fun (removed, rest) ->
                let afterPop =
                    queues
                    |> List.removeAt firstPlaceWithInstruction
                    |> List.insertAt firstPlaceWithInstruction rest

                removed, afterPop
            )
        )

    let rec allOrderings (queues : 'a list list) : (int * 'a) list list =
        let output = popOne queues

        match output with
        | [] -> [ [] ]
        | output ->

        output
        |> List.collect (fun (extracted, remaining) ->
            let sub = allOrderings remaining
            sub |> List.map (fun s -> extracted :: s)
        )

    let factorial i =
        let rec go acc i =
            if i <= 0 then acc else go (acc * i) (i - 1)

        go 1 i

    [<TestCase(0, 1)>]
    [<TestCase(1, 1)>]
    [<TestCase(2, 2)>]
    [<TestCase(3, 6)>]
    [<TestCase(4, 24)>]
    let ``Test factorial`` (n : int, result : int) = factorial n |> shouldEqual result

    [<Test>]
    let ``Test allOrderings`` () =
        let case = [ [ "a" ; "b" ] ; [ "c" ; "d" ; "e" ] ]
        let output = case |> allOrderings
        output |> shouldEqual (List.distinct output)

        output
        |> List.length
        |> shouldEqual (factorial (List.concat case |> List.length))

        let allElements = Set.ofList (List.concat case)

        for output in output do
            output |> List.map snd |> Set.ofList |> shouldEqual allElements

    let randomChoice<'a> (r : System.Random) (arr : 'a list) : 'a = arr.[r.Next (0, arr.Length)]

    [<Test>]
    let ``Startup sequence in prod, two timeouts at once, random`` () =
        let rand = System.Random ()
        let cluster, network = InMemoryCluster.make<int> 5

        NetworkAction.InactivityTimeout 0<ServerId>
        |> NetworkAction.perform cluster network

        NetworkAction.InactivityTimeout 1<ServerId>
        |> NetworkAction.perform cluster network

        // Those two each sent a message to every other server.
        (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual 1

        (network.AllInboundMessages 1<ServerId>).Length |> shouldEqual 1

        for i in 2..4 do
            let server = i * 1<ServerId>

            (network.AllInboundMessages server).Length |> shouldEqual 2

        while network.AllUndeliveredMessages () |> Seq.concat |> Seq.isEmpty |> not do
            let allOrderings' =
                network.AllUndeliveredMessages () |> List.map List.ofSeq |> allOrderings

            // Process the messages!
            let ordering = randomChoice rand allOrderings'

            for serverConsuming, (messageId, message) in ordering do
                let serverConsuming = serverConsuming * 1<ServerId>
                cluster.SendMessageDirectly serverConsuming message
                network.DropMessage serverConsuming messageId

        match cluster.Servers.[0].State, cluster.Servers.[1].State with
        | Leader _, Leader _ -> failwith "Unexpectedly had two leaders"
        | Candidate _, Candidate _ -> failwith "Unexpectedly failed to elect a leader"
        | Leader 1<Term>, Follower
        | Follower, Leader 1<Term> -> ()
        | s1, s2 -> failwithf "Unexpected state: %O %O" s1 s2

        for i in 2..4 do
            cluster.Servers.[i].State |> shouldEqual ServerStatus.Follower

    type NetworkMessageSelection =
        | NetworkMessageSelection of (int<ServerId> * int) list

        member this.Length =
            match this with
            | NetworkMessageSelection h -> List.length h

    let networkMessageSelectionGen (clusterSize : int) : Gen<NetworkMessageSelection> =
        gen {
            let! pile = Gen.choose (0, clusterSize - 1)
            let! entry = Arb.generate<int>
            return (pile * 1<ServerId>, abs entry)
        }
        |> Gen.listOf
        |> Gen.map NetworkMessageSelection

    let apply (NetworkMessageSelection history) (cluster : Cluster<'a>) (network : Network<'a>) : unit =
        for pile, entry in history do
            let messages = network.AllInboundMessages pile

            if entry < messages.Length then
                cluster.SendMessageDirectly pile messages.[entry]

    [<Test>]
    let ``Startup sequence in prod, two timeouts at once, property-based: at most one leader is elected`` () =
        let property (history : NetworkMessageSelection) =
            let cluster, network = InMemoryCluster.make<int> 5

            NetworkAction.InactivityTimeout 0<ServerId>
            |> NetworkAction.perform cluster network

            NetworkAction.InactivityTimeout 1<ServerId>
            |> NetworkAction.perform cluster network

            // Those two each sent a message to every other server.
            (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual 1

            (network.AllInboundMessages 1<ServerId>).Length |> shouldEqual 1

            for i in 2..4 do
                (network.AllInboundMessages (i * 1<ServerId>)).Length |> shouldEqual 2

            apply history cluster network

            match cluster.Servers.[0].State, cluster.Servers.[1].State with
            | Leader _, Leader _ -> failwith "Unexpectedly elected two leaders"
            | _, _ -> ()

            for i in 2..4 do
                cluster.Servers.[i].State |> shouldEqual ServerStatus.Follower

        property |> Prop.forAll (Arb.fromGen (networkMessageSelectionGen 5)) |> check

    [<Test>]
    let ``Data can propagate from the leader`` () =
        let clusterSize = 5
        let cluster, network = InMemoryCluster.make<byte> clusterSize

        let registeredSuccessfully = ref 0

        let registerResponse (response : RegisterClientResponse) : unit =
            response |> shouldEqual (RegisterClientResponse.Success 1<ClientId>)
            Interlocked.Increment registeredSuccessfully |> ignore

        let respondedSuccessfully = ref 0

        let requestResponse (response : ClientResponse) : unit =
            response
            |> shouldEqual (ClientResponse.Success (1<ClientId>, 0<ClientSequence>))

            Interlocked.Increment respondedSuccessfully |> ignore

        let startupSequence =
            [
                NetworkAction.InactivityTimeout 1<ServerId>
                // Two servers vote for server 1...
                NetworkAction.NetworkMessage (2<ServerId>, 0)
                NetworkAction.NetworkMessage (3<ServerId>, 0)
                // Server 1 processes incoming votes, and achieves majority, electing itself leader!
                NetworkAction.NetworkMessage (1<ServerId>, 0)
                NetworkAction.NetworkMessage (1<ServerId>, 1)
                // and the other votes are processed and discarded
                NetworkAction.NetworkMessage (0<ServerId>, 0)
                NetworkAction.NetworkMessage (4<ServerId>, 0)
                NetworkAction.NetworkMessage (1<ServerId>, 2)
                NetworkAction.NetworkMessage (1<ServerId>, 3)
                // Get the followers' heartbeat processing out of the way
                NetworkAction.NetworkMessage (0<ServerId>, 1)
                NetworkAction.NetworkMessage (2<ServerId>, 1)
                NetworkAction.NetworkMessage (3<ServerId>, 1)
                NetworkAction.NetworkMessage (4<ServerId>, 1)
                NetworkAction.NetworkMessage (1<ServerId>, 4)
                NetworkAction.NetworkMessage (1<ServerId>, 5)
                NetworkAction.NetworkMessage (1<ServerId>, 6)
                NetworkAction.NetworkMessage (1<ServerId>, 7)

                // Create a client.
                NetworkAction.ClientRequest (1<ServerId>, ClientRequest.RegisterClient registerResponse)
                NetworkAction.NetworkMessage (0<ServerId>, 2)
                NetworkAction.NetworkMessage (2<ServerId>, 2)
                NetworkAction.NetworkMessage (3<ServerId>, 2)
                NetworkAction.NetworkMessage (4<ServerId>, 2)
            ]

        for action in startupSequence do
            NetworkAction.perform cluster network action

        let leader = 1<ServerId>

        // Server 1 is the only leader.
        cluster.Leaders |> Seq.exactlyOne |> shouldEqual leader

        // No outstanding messages except to the leader.
        for i in 0 .. clusterSize - 1 do
            let i = i * 1<ServerId>

            if i <> leader then
                network.UndeliveredMessages i |> shouldBeEmpty

        // The leader has yet to receive the acknowledgements.
        let undelivered =
            network.UndeliveredMessages leader
            |> List.map (fun (i, message) ->
                match message with
                | Message.Reply (Reply.AppendEntriesReply r) ->
                    r.FollowerTerm |> shouldEqual 1<Term>
                    r.Success |> Option.get |> shouldEqual 1<LogIndex>
                    i, r.Follower
                | _ -> failwith "oh no"
            )

        undelivered
        |> List.map snd
        |> shouldEqual (
            [ 0 .. clusterSize - 1 ]
            |> List.map ((*) 1<ServerId>)
            |> List.filter ((<>) leader)
        )

        // The client has not received an acknowledgement.
        respondedSuccessfully.Value |> shouldEqual 0
        registeredSuccessfully.Value |> shouldEqual 0

        // Now tell the leader that the followers have accepted the client.
        undelivered
        |> List.iter (fun (count, _) ->
            NetworkAction.perform cluster network (NetworkAction.NetworkMessage (leader, count))
        )

        // The client now knows it exists!
        registeredSuccessfully.Value |> shouldEqual 1
        respondedSuccessfully.Value |> shouldEqual 0

        // Submit some client data.
        NetworkAction.ClientRequest (
            1<ServerId>,
            ClientRequest.ClientRequest (1<ClientId>, 0<ClientSequence>, 99uy, requestResponse)
        )
        |> NetworkAction.perform cluster network

        // Perform data-propagating heartbeats.
        for i in 0 .. clusterSize - 1 do
            let server = i * 1<ServerId>

            NetworkAction.NetworkMessage (server, 3)
            |> NetworkAction.perform cluster network

        // The client hasn't yet received a response, because the leader hasn't heard back from the cluster.
        registeredSuccessfully.Value |> shouldEqual 1
        respondedSuccessfully.Value |> shouldEqual 0

        let awaiting =
            network.UndeliveredMessages leader
            |> List.map (fun (i, message) ->
                match message with
                | Message.Reply (Reply.AppendEntriesReply r) ->
                    r.FollowerTerm |> shouldEqual 1<Term>
                    // Note the increased log index from last time.
                    r.Success |> Option.get |> shouldEqual 2<LogIndex>
                    i, r.Follower
                | _ -> failwith "oh no"
            )

        awaiting
        |> List.head
        |> fun (messageIndex, _) ->
            NetworkAction.NetworkMessage (leader, messageIndex)
            |> NetworkAction.perform cluster network

        // Leader doesn't know a quorum has been reached, so does not reply to the client.
        registeredSuccessfully.Value |> shouldEqual 1
        respondedSuccessfully.Value |> shouldEqual 0

        awaiting.[1]
        |> fun (messageIndex, _) ->
            NetworkAction.NetworkMessage (leader, messageIndex)
            |> NetworkAction.perform cluster network

        // Quorum achieved! Reply sent.
        registeredSuccessfully.Value |> shouldEqual 1
        respondedSuccessfully.Value |> shouldEqual 1

        awaiting.[2..]
        |> List.iter (fun (messageIndex, _) ->
            NetworkAction.NetworkMessage (leader, messageIndex)
            |> NetworkAction.perform cluster network
        )

        registeredSuccessfully.Value |> shouldEqual 1
        respondedSuccessfully.Value |> shouldEqual 1

    let freeze<'a> (cluster : Cluster<'a>) =
        List.init
            cluster.ClusterSize
            (fun i ->
                let i = i * 1<ServerId>
                Async.RunSynchronously (cluster.GetCurrentInternalState i), cluster.Status i
            )

    let replay<'a> (ValidHistory history : ValidHistory<'a>) (cluster : Cluster<'a>) (network : Network<'a>) : unit =
        for h in history do
            NetworkAction.perform cluster network h

    [<Test>]
    let ``History can be replayed`` () =
        let clusterSize = 5

        let property (history : ValidHistory<byte>) =
            let firstTime =
                let cluster, network = InMemoryCluster.make<byte> clusterSize
                replay history cluster network
                freeze cluster

            let secondTime =
                let cluster, network = InMemoryCluster.make<byte> clusterSize
                replay history cluster network
                freeze cluster

            firstTime = secondTime

        property
        |> Prop.forAll (ValidHistory.arb (Arb.Default.Byte().Generator) clusterSize)
        |> check


    [<Test>]
    let ``There is never more than one leader in the same term`` () =
        let clusterSize = 5

        let property (history : ValidHistory<byte>) : bool =
            let cluster, network = InMemoryCluster.make<byte> clusterSize
            replay history cluster network

            let leaders =
                freeze cluster
                |> List.choose (fun (_, status) ->
                    match status with
                    | ServerStatus.Leader term -> Some term
                    | _ -> None
                )

            List.distinct leaders = leaders

        property
        |> Prop.forAll (ValidHistory.arb (Arb.Default.Byte().Generator) clusterSize)
        |> check

    let duplicationProperty<'a when 'a : equality>
        (clusterSize : int)
        (beforeDuplication : ValidHistory<'a>, afterDuplication : ValidHistory<'a>)
        : bool
        =
        let withoutDuplicate =
            let cluster, network = InMemoryCluster.make<'a> clusterSize
            replay beforeDuplication cluster network
            freeze cluster

        let withDuplicate =
            let cluster, network = InMemoryCluster.make<'a> clusterSize
            replay afterDuplication cluster network
            freeze cluster

        withDuplicate = withoutDuplicate

    let possibleDuplicates<'a> (history : NetworkAction<'a> list) : (int * NetworkAction<'a>) list =
        history
        |> List.indexed
        |> List.filter (fun (_, action) ->
            match action with
            | NetworkAction.DropMessage _ -> true
            | NetworkAction.Heartbeat _ -> true
            | NetworkAction.NetworkMessage _ -> true
            | NetworkAction.InactivityTimeout _ ->
                // This starts a new term, so is not safe to repeat.
                false
            | NetworkAction.ClientRequest _ ->
                // Clients repeating requests may of course change state!
                false
        )

    let allDuplicatedHistories<'a>
        (clusterSize : int)
        (ValidHistory historyList : ValidHistory<'a> as history)
        : _ list
        =
        let duplicateCandidates = possibleDuplicates historyList

        duplicateCandidates
        |> List.collect (fun (index, itemToDuplicate) ->
            [ index .. historyList.Length ]
            |> List.choose (fun insertIndex ->
                List.insertAt insertIndex itemToDuplicate historyList
                |> ValidHistory.validate clusterSize
                |> Option.map (fun withDuplicate -> history, withDuplicate)
            )
        )

    let rec withDuplicateGen<'a> (elementGen : Gen<'a>) (clusterSize : int) : Gen<ValidHistory<'a> * ValidHistory<'a>> =
        gen {
            let! history = ValidHistory.gen elementGen clusterSize
            let allDuplicatedHistories = allDuplicatedHistories<'a> clusterSize history

            match allDuplicatedHistories with
            | [] -> return! withDuplicateGen elementGen clusterSize
            | x -> return! Gen.elements x
        }

    let duplicationArb<'a> (elementGen : Gen<'a>) (clusterSize : int) : Arbitrary<ValidHistory<'a> * ValidHistory<'a>> =
        { new Arbitrary<_>() with
            member _.Generator = withDuplicateGen<'a> elementGen clusterSize

            member _.Shrinker ((before, _withDuplicate)) =
                ValidHistory.shrink<'a> clusterSize before
                |> Seq.collect (allDuplicatedHistories clusterSize)
        }

(*
    TODO: the following tests are borked; see the "specific example" for why.
    [<Test>]
    let ``Duplicate messages don't change network state`` () =
        let clusterSize = 5

        duplicationProperty<byte> clusterSize
        |> Prop.forAll (duplicationArb (Arb.Default.Byte().Generator) clusterSize)
        |> check

    [<Test>]
    let ``Specific example`` () =
        let clusterSize = 5

        let history =
            [
                InactivityTimeout 4<ServerId>
                InactivityTimeout 3<ServerId>
                NetworkMessage (0<ServerId>, 1)
                InactivityTimeout 4<ServerId>
                NetworkMessage (3<ServerId>, 2)
            ]
            |> ValidHistory.validate<byte> clusterSize
            |> Option.get

        let withDuplicate =
            [
                InactivityTimeout 4<ServerId>
                InactivityTimeout 3<ServerId>
                NetworkMessage (0<ServerId>, 1)
                NetworkMessage (0<ServerId>, 1)
                InactivityTimeout 4<ServerId>
                // TODO: this is the problem, 2 no longer refers to the
                // same
                NetworkMessage (3<ServerId>, 2)
            ]
            |> ValidHistory.validate<byte> clusterSize
            |> Option.get

        duplicationProperty clusterSize (history, withDuplicate) |> shouldEqual true
    *)
