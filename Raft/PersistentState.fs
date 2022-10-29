namespace Raft

open System.Threading

type IPersistentState<'a> =
    abstract CurrentTerm : int<Term>
    /// If I know about an election in my CurrentTerm, who did I vote for during that election?
    abstract VotedFor : int<ServerId> option
    abstract AppendToLog : 'a -> int<Term> -> unit

    /// Truncate away the most recent entries of the log.
    /// If `GetLogEntry x` would succeed, and then we call `TruncateLog x`,
    /// then `GetLogEntry x` will still succeed (but `GetLogEntry (x + 1)` will not).
    abstract TruncateLog : int<LogIndex> -> unit
    abstract GetLogEntry : int<LogIndex> -> ('a * int<Term>) option
    abstract CurrentLogIndex : int<LogIndex>
    abstract GetLastLogEntry : unit -> ('a * LogEntry) option
    abstract AdvanceToTerm : int<Term> -> unit
    abstract IncrementTerm : unit -> unit
    abstract Vote : int<ServerId> -> unit

/// Server state which must survive a server crash.
[<Class>]
type InMemoryPersistentState<'a> () =
    let mutable currentTerm = 0
    let mutable votedFor : int<ServerId> option = None
    let log = ResizeArray<'a * int<Term>> ()

    member this.GetLog () = log |> List.ofSeq

    interface IPersistentState<'a> with
        member this.CurrentTerm = currentTerm * 1<Term>

        member this.IncrementTerm () =
#if FABLE_COMPILER
            currentTerm <- currentTerm + 1
#else
            Interlocked.Increment &currentTerm |> ignore
#endif

        member this.VotedFor = votedFor
        member this.Vote id = votedFor <- Some id

        member this.AdvanceToTerm term =
            currentTerm <- term / 1<Term>
            votedFor <- None

        member this.AppendToLog entry term = log.Add (entry, term)

        member this.TruncateLog position =
            let position = position / 1<LogIndex>

            if position < log.Count then
                let position = if position < 0 then 0 else position
                log.RemoveRange (position, log.Count - position)

        member this.GetLastLogEntry () : ('a * LogEntry) option =
            if log.Count = 0 then
                None
            else
                let stored, term = log.[log.Count - 1]

                Some (
                    stored,
                    {
                        Index = log.Count * 1<LogIndex>
                        Term = term
                    }
                )

        member this.GetLogEntry position =
            let position = position / 1<LogIndex>

            if log.Count < position then None
            elif position <= 0 then None
            else Some log.[position - 1]

        member this.CurrentLogIndex = log.Count * 1<LogIndex>
