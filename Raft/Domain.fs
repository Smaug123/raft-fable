namespace Raft

/// LogIndex is indexed from 1. We use 0 to indicate "before any history has started".
[<Measure>]
type LogIndex

[<Measure>]
type Term

[<Measure>]
type ServerId

type LogEntry =
    {
        Index : int<LogIndex>
        Term : int<Term>
    }

    override this.ToString () =
        sprintf "Log entry %i at subjective term %i" this.Index this.Term
