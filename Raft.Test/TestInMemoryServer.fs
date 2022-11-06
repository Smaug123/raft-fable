namespace Raft.Test

open System.Threading
open Raft
open NUnit.Framework
open FsUnitTyped
open FsCheck

[<TestFixture>]
module TestInMemoryServer =

    [<Test>]
    let ``Startup sequence, first fumbling steps`` () =
        let cluster, network = InMemoryCluster.make<int> 5

        let logger, logs = TestLogger.make ()

        // Candidate 1 asks server 0 to vote for it.

        {
            CandidateTerm = 0<Term>
            CandidateId = 1<ServerId>
            ReplyChannel = fun message -> logger (sprintf "Received message for term %i" message.VoterTerm)
            CandidateLastLogEntry = None
        }
        |> Instruction.RequestVote
        |> Message.Instruction
        |> cluster.SendMessageDirectly 0<ServerId>

        logs () |> shouldEqual [ "Received message for term 0" ]

        // Candidate 1 asks to be elected again! This is fine, maybe the network is replaying requests
        // and the network swallowed our reply, so we should reply in the same way.
        {
            CandidateTerm = 0<Term>
            CandidateId = 1<ServerId>
            ReplyChannel = fun message -> logger (sprintf "Received message for term %i" message.VoterTerm)
            CandidateLastLogEntry = None
        }
        |> Instruction.RequestVote
        |> Message.Instruction
        |> cluster.SendMessageDirectly 0<ServerId>

        logs ()
        |> shouldEqual [ "Received message for term 0" ; "Received message for term 0" ]

        // Candidate 2 asks to be elected. We won't vote for them, because we've already voted.
        // and the network swallowed our reply, so we should reply in the same way.
        let calls = ref 0

        {
            CandidateTerm = 0<Term>
            CandidateId = 2<ServerId>
            ReplyChannel = fun _ -> Interlocked.Increment calls |> ignore
            CandidateLastLogEntry = None
        }
        |> Instruction.RequestVote
        |> Message.Instruction
        |> cluster.SendMessageDirectly 0<ServerId>

        calls.Value |> shouldEqual 0

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

    let check (prop : Property) =
        let config =
            { Config.QuickThrowOnFailure with
                MaxTest = 1000
            }

        Check.One (config, prop)

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

        let mutable replyChannel = None

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
                // Submit data to leader. This has the effect of heartbeating the other
                // nodes, with a heartbeat that contains the new data.
                NetworkAction.ClientRequest (1<ServerId>, byte 3, (fun s -> replyChannel <- Some s))

                // Deliver the data messages.
                NetworkAction.NetworkMessage (0<ServerId>, 2)
                NetworkAction.NetworkMessage (2<ServerId>, 2)
                NetworkAction.NetworkMessage (3<ServerId>, 2)
                NetworkAction.NetworkMessage (4<ServerId>, 2)
            ]

        for action in startupSequence do
            NetworkAction.perform cluster network action

        replyChannel |> Option.get |> shouldEqual ClientReply.Acknowledged

        // The servers have all accepted the data.
        network.UndeliveredMessages 1<ServerId>
        |> List.map (fun (_index, message) ->
            match message with
            | Message.Reply (Reply.AppendEntriesReply reply) -> reply
            | _ -> failwithf "Unexpected reply: %+A" message
        )
        |> shouldEqual
            [
                {
                    Success = Some 1<LogIndex>
                    Follower = 0<ServerId>
                    FollowerTerm = 1<Term>
                }
                {
                    Success = Some 1<LogIndex>
                    Follower = 2<ServerId>
                    FollowerTerm = 1<Term>
                }
                {
                    Success = Some 1<LogIndex>
                    Follower = 3<ServerId>
                    FollowerTerm = 1<Term>
                }
                {
                    Success = Some 1<LogIndex>
                    Follower = 4<ServerId>
                    FollowerTerm = 1<Term>
                }
            ]

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

        property |> Prop.forAll (ValidHistory.arb clusterSize) |> check


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

        property |> Prop.forAll (ValidHistory.arb clusterSize) |> check

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

    let rec withDuplicateGen<'a> (clusterSize : int) : Gen<ValidHistory<'a> * ValidHistory<'a>> =
        gen {
            let! history = ValidHistory.gen clusterSize
            let allDuplicatedHistories = allDuplicatedHistories<'a> clusterSize history

            match allDuplicatedHistories with
            | [] -> return! withDuplicateGen clusterSize
            | x -> return! Gen.elements x
        }

    let duplicationArb<'a> (clusterSize : int) : Arbitrary<ValidHistory<'a> * ValidHistory<'a>> =
        { new Arbitrary<_>() with
            member _.Generator = withDuplicateGen<'a> clusterSize

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
        |> Prop.forAll (duplicationArb clusterSize)
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
