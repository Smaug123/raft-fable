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
            match s.GetLogEntry (i * 1<LogIndex>) with
            | Some _ -> failwith "should not have had a log entry"
            | None -> ()

        s.CurrentTerm |> shouldEqual 0<Term>
        s.VotedFor |> shouldEqual None

        match s.GetLastLogEntry () with
        | Some _ -> failwith "should not have had a log entry"
        | None -> ()

    let ofList<'a> (xs : ('a * int<Term>) list) : InMemoryPersistentState<'a> =
        let s = InMemoryPersistentState<'a> ()

        for x, term in xs do
            (s :> IPersistentState<_>).AppendToLog
                (LogEntry.ClientEntry (x, 1<ClientId>, 1<ClientSequence>, ignore))
                term

        s

    let isPrefix (prefix : 'a list) (l : 'a list) : bool =
        l
        |> List.truncate prefix.Length
        |> List.zip prefix
        |> List.forall (fun (x, y) -> x = y)

    [<Test>]
    let ``Nonzero truncation followed by Get succeeds`` () =
        let property (truncate : int<LogIndex>) (xs : (byte * int<Term>) list) : bool =
            let truncate = abs truncate + 1<LogIndex>
            let uut = ofList xs

            let oldLog =
                uut.GetLog ()
                |> List.map (fun (entry, term) -> SerialisedLogEntry.Make entry, term)

            match (uut :> IPersistentState<_>).GetLogEntry truncate with
            | None ->
                (uut :> IPersistentState<_>).TruncateLog truncate

                let newLog =
                    uut.GetLog ()
                    |> List.map (fun (entry, term) -> SerialisedLogEntry.Make entry, term)

                newLog = oldLog
            | Some (itemStored, entry) ->
                (uut :> IPersistentState<_>).TruncateLog truncate

                let newLog =
                    uut.GetLog ()
                    |> List.map (fun (entry, term) -> SerialisedLogEntry.Make entry, term)

                let retrieved, logEntry =
                    Option.get ((uut :> IPersistentState<_>).GetLastLogEntry ())

                logEntry = {
                               Index = truncate
                               Term = entry
                           }
                && (SerialisedLogEntry.Make retrieved = SerialisedLogEntry.Make itemStored)
                && isPrefix newLog oldLog
                && (uut :> IPersistentState<_>).CurrentLogIndex = truncate

        Check.QuickThrowOnFailure property

    [<Test>]
    let ``Zero truncation results in empty log`` () =
        let property (truncate : int<LogIndex>) (xs : (int * int<Term>) list) : bool =
            let truncate = -abs truncate
            let uut = ofList xs

            // It's not meaningful to take the 0th element
            match (uut :> IPersistentState<_>).GetLogEntry truncate with
            | Some _ -> failwith "should not have had any elements"
            | None -> ()

            (uut :> IPersistentState<_>).TruncateLog truncate

            match uut.GetLog () with
            | [] -> ()
            | _ -> failwith "should not have had log entries"

            let uut = uut :> IPersistentState<_>

            match uut.GetLastLogEntry () with
            | Some _ -> false
            | None -> uut.CurrentLogIndex = 0<LogIndex>

        Check.QuickThrowOnFailure property
