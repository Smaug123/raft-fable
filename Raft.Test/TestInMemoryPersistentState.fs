namespace Raft.Test

open NUnit.Framework
open Raft
open FsUnitTyped
open FsCheck

[<TestFixture>]
module TestInMemoryPersistentState =

    [<Test>]
    let ``Properties of empty`` () =
        let s = InMemoryPersistentState<int> () :> IPersistentState<_>

        s.CurrentLogIndex |> shouldEqual 0<LogIndex>

        for i in -2 .. 10 do
            s.GetLogEntry (i * 1<LogIndex>) |> shouldEqual None

        s.CurrentTerm |> shouldEqual 0<Term>
        s.VotedFor |> shouldEqual None

        s.GetLastLogEntry () |> shouldEqual None

    let ofList<'a> (xs : ('a * int<Term>) list) : InMemoryPersistentState<'a> =
        let s = InMemoryPersistentState<'a> ()

        for x, term in xs do
            (s :> IPersistentState<_>).AppendToLog x term

        s

    let isPrefix (prefix : 'a list) (l : 'a list) : bool =
        l
        |> List.truncate prefix.Length
        |> List.zip prefix
        |> List.forall (fun (x, y) -> x = y)

    [<Test>]
    let ``Nonzero truncation followed by Get succeeds`` () =
        let property (truncate : int<LogIndex>) (xs : (int * int<Term>) list) : bool =
            let truncate = abs truncate + 1<LogIndex>
            let uut = ofList xs
            let oldLog = uut.GetLog ()

            match (uut :> IPersistentState<_>).GetLogEntry truncate with
            | None ->
                (uut :> IPersistentState<_>).TruncateLog truncate
                uut.GetLog () = oldLog
            | Some entry ->
                (uut :> IPersistentState<_>).TruncateLog truncate

                (uut :> IPersistentState<_>).GetLastLogEntry () = Some (truncate, entry)
                && isPrefix (uut.GetLog ()) oldLog
                && (uut :> IPersistentState<_>).CurrentLogIndex = truncate

        Check.QuickThrowOnFailure property

    [<Test>]
    let ``Zero truncation results in empty log`` () =
        let property (truncate : int<LogIndex>) (xs : (int * int<Term>) list) : bool =
            let truncate = -abs truncate
            let uut = ofList xs

            // It's not meaningful to take the 0th element
            (uut :> IPersistentState<_>).GetLogEntry truncate |> shouldEqual None

            (uut :> IPersistentState<_>).TruncateLog truncate

            uut.GetLog () |> shouldEqual []

            let uut = uut :> IPersistentState<_>
            uut.GetLastLogEntry () = None && uut.CurrentLogIndex = 0<LogIndex>

        Check.QuickThrowOnFailure property
