namespace Raft

/// LogIndex is indexed from 1. We use 0 to indicate "before any history has started".
[<Measure>]
type LogIndex

[<Measure>]
type Term

[<Measure>]
type ServerId
