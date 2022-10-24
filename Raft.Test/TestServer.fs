namespace Raft.Test

open System.Threading
open Raft
open NUnit.Framework
open FsUnitTyped

[<TestFixture>]
module TestServer =

    [<Test>]
    let foo () =
        let cluster = InMemoryCluster.make<int> 5

        let logger, logs =
            let logs = ResizeArray ()
            let logLine (s : string) = lock logs (fun () -> logs.Add s)

            let freezeLogs () =
                lock logs (fun () -> logs |> Seq.toList)

            logLine, freezeLogs

        let sendMessage = cluster.Servers.[0].OutboundMessageChannel

        // Candidate 1 asks server 0 to vote for it.

        {
            CandidateTerm = 0<Term>
            CandidateId = 1<ServerId>
            ReplyChannel = fun message -> logger (sprintf "Received message for term %i" message.VoterTerm)
            CandidateLastLogEntry = 0<LogIndex>, 0<Term>
        }
        |> Message.RequestVote
        |> sendMessage 0<ServerId>

        logs () |> shouldEqual [ "Received message for term 0" ]

        // Candidate 1 asks to be elected again! This is fine, maybe the network is replaying requests
        // and the network swallowed our reply, so we should reply in the same way.
        {
            CandidateTerm = 0<Term>
            CandidateId = 1<ServerId>
            ReplyChannel = fun message -> logger (sprintf "Received message for term %i" message.VoterTerm)
            CandidateLastLogEntry = 0<LogIndex>, 0<Term>
        }
        |> Message.RequestVote
        |> sendMessage 0<ServerId>

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
        |> Message.RequestVote
        |> sendMessage 0<ServerId>

        calls.Value |> shouldEqual 0
