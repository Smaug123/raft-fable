namespace Raft.Test

open System.Threading
open Raft
open NUnit.Framework
open FsUnitTyped

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
    let ``Startup sequence in prod`` () =
        let cluster, network = InMemoryCluster.make<int> 5

        cluster.Servers.[0].TriggerTimeout ()
        cluster.Servers.[0].Sync ()

        // We sent a message to every other server; process them.
        for i in 1..4 do
            network.InboundMessages.[i].Count |> shouldEqual 1
            let message = network.InboundMessages.[i].[0]
            network.InboundMessages.[i].Clear ()
            cluster.SendMessageDirectly (i * 1<ServerId>) message

            network.InboundMessages.[0].Count |> shouldEqual i

        for i in 1..4 do
            cluster.SendMessageDirectly 0<ServerId> network.InboundMessages.[0].[i - 1]
        // (the messages we've already processed)
        network.InboundMessages.[0].Count |> shouldEqual 4
        network.InboundMessages.[0].Clear ()

        cluster.Servers.[0].State |> shouldEqual ServerStatus.Leader

        for i in 1..4 do
            cluster.Servers.[i].State |> shouldEqual ServerStatus.Follower
