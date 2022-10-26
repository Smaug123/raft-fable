namespace Raft.Test

open System.Threading
open Raft
open NUnit.Framework
open FsUnitTyped
open FsCheck

[<TestFixture>]
module TestServer =

    [<Test>]
    let ``Startup sequence, first fumbling steps`` () =
        let cluster, network = InMemoryCluster.make<int> 5

        let logger, logs = TestLogger.make ()

        // Candidate 1 asks server 0 to vote for it.

        {
            CandidateTerm = 0<Term>
            CandidateId = 1<ServerId>
            ReplyChannel = fun message -> logger (sprintf "Received message for term %i" message.VoterTerm)
            CandidateLastLogEntry = 0<LogIndex>, 0<Term>
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
            CandidateLastLogEntry = 0<LogIndex>, 0<Term>
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
            CandidateLastLogEntry = 0<LogIndex>, 0<Term>
        }
        |> Instruction.RequestVote
        |> Message.Instruction
        |> cluster.SendMessageDirectly 0<ServerId>

        calls.Value |> shouldEqual 0

    [<Test>]
    let ``Startup sequence in prod, only one timeout takes place`` () =
        let cluster, network = InMemoryCluster.make<int> 5

        cluster.Servers.[0].TriggerTimeout ()
        cluster.Servers.[0].Sync ()

        // We sent a message to every other server; process them.
        for i in 1..4 do
            let server = i * 1<ServerId>
            (network.AllInboundMessages server).Length |> shouldEqual 1
            let message = network.InboundMessage server 0
            network.DropMessage server 0
            cluster.SendMessageDirectly server message

            (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual i

        for i in 1..4 do
            network.InboundMessage 0<ServerId> (i - 1)
            |> cluster.SendMessageDirectly 0<ServerId>

            network.DropMessage 0<ServerId> (i - 1)

        // (the messages we've already processed)
        (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual 4
        (network.UndeliveredMessages 0<ServerId>).Length |> shouldEqual 0

        cluster.Servers.[0].State |> shouldEqual ServerStatus.Leader

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

        cluster.Servers.[0].TriggerTimeout ()
        cluster.Servers.[0].Sync ()
        cluster.Servers.[1].TriggerTimeout ()
        cluster.Servers.[1].Sync ()

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

        (cluster.Servers.[0].State = Leader && cluster.Servers.[1].State = Leader)
        |> shouldEqual false

        (cluster.Servers.[0].State = Candidate && cluster.Servers.[1].State = Candidate)
        |> shouldEqual false

        ((cluster.Servers.[0].State = Leader && cluster.Servers.[1].State = Candidate)
         || (cluster.Servers.[1].State = Leader && cluster.Servers.[0].State = Candidate))
        |> shouldEqual true

        for i in 2..4 do
            cluster.Servers.[i].State |> shouldEqual ServerStatus.Follower

    type History = History of (int<ServerId> * int) list

    let historyGen (clusterSize : int) =
        gen {
            let! pile = Gen.choose (0, clusterSize - 1)
            let! entry = Arb.generate<int>
            return (pile * 1<ServerId>, abs entry)
        }
        |> Gen.listOf
        |> Gen.map History

    let apply (History history) (cluster : Cluster<'a>) (network : Network<'a>) : unit =
        for pile, entry in history do
            let messages = network.AllInboundMessages pile

            if entry < messages.Length then
                cluster.SendMessageDirectly pile messages.[entry]

    [<Test>]
    let ``Startup sequence in prod, two timeouts at once, property-based: at most one leader is elected`` () =
        let cluster, network = InMemoryCluster.make<int> 5

        cluster.Servers.[0].TriggerTimeout ()
        cluster.Servers.[0].Sync ()
        cluster.Servers.[1].TriggerTimeout ()
        cluster.Servers.[1].Sync ()

        // Those two each sent a message to every other server.
        (network.AllInboundMessages 0<ServerId>).Length |> shouldEqual 1
        (network.AllInboundMessages 1<ServerId>).Length |> shouldEqual 1

        for i in 2..4 do
            (network.AllInboundMessages (i * 1<ServerId>)).Length |> shouldEqual 2

        let property (history : History) =
            apply history cluster network

            (cluster.Servers.[0].State = Leader && cluster.Servers.[1].State = Leader)
            |> shouldEqual false

            for i in 2..4 do
                cluster.Servers.[i].State |> shouldEqual ServerStatus.Follower

        property
        |> Prop.forAll (Arb.fromGen (historyGen 5))
        |> Check.QuickThrowOnFailure
