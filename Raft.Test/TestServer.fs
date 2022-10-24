namespace Raft.Test

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

        // Candidate 1 asks to be elected.

        {
            CandidateTerm = 0<Term>
            CandidateId = 1<ServerId>
            ReplyChannel = fun message -> logger (sprintf "Received message for term %i" message.VoterTerm)
            CandidateLastLogEntry = 0<LogIndex>, 0<Term>
        }
        |> Message.RequestVote
        |> sendMessage 0<ServerId>

        logs () |> shouldEqual [ "Received message for term 0" ]
