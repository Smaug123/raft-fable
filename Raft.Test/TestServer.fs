namespace Raft.Test

open FsUnitTyped
open NUnit.Framework
open Raft

[<TestFixture>]
module TestServer =

    [<Test>]
    let ``maxLogAQuorumHasCommitted tests`` () =
        for length in 1..7 do
            for i in 0..10 do
                let i = i * 1<LogIndex>

                i
                |> Array.create length
                |> ServerUtils.maxLogAQuorumHasCommitted
                |> shouldEqual i

        ServerUtils.maxLogAQuorumHasCommitted [| 0<LogIndex> ; 0<LogIndex> ; 1<LogIndex> |]
        |> shouldEqual 0<LogIndex>

        ServerUtils.maxLogAQuorumHasCommitted [| 0<LogIndex> ; 1<LogIndex> ; 1<LogIndex> |]
        |> shouldEqual 1<LogIndex>

        ServerUtils.maxLogAQuorumHasCommitted [| 2<LogIndex> ; 1<LogIndex> ; 1<LogIndex> |]
        |> shouldEqual 1<LogIndex>

        ServerUtils.maxLogAQuorumHasCommitted [| 2<LogIndex> ; 1<LogIndex> ; 2<LogIndex> |]
        |> shouldEqual 2<LogIndex>

        ServerUtils.maxLogAQuorumHasCommitted [| 1<LogIndex> ; 2<LogIndex> ; 3<LogIndex> ; 4<LogIndex> |]
        |> shouldEqual 2<LogIndex>
