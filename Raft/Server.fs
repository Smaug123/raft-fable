namespace Raft

/// Server state which need not survive a server crash.
type VolatileState =
    {
        /// The index of the highest log entry we know is persisted to a majority of the cluster.
        // Why is it correct for this to be volatile?
        // Answer: it's representing the fact that if we restart and the CommitIndex is set back to 0, indeed
        // we *don't* know that any of our log is reflected in the other nodes.
        // (We'll soon learn a better value of CommitIndex as we start receiving messages again.)
        CommitIndex : int<LogIndex>
        LastApplied : int<LogIndex>
    }

    static member New : VolatileState =
        {
            CommitIndex = 0<LogIndex>
            LastApplied = 0<LogIndex>
        }

type LeaderState =
    {
        /// For each server, index of the next log entry to send to that server
        NextIndex : int<LogIndex> array
        /// For each server, index of the highest log entry known to be replicated on that server
        MatchIndex : int array
    }

/// You asked me to vote for you to become leader. Here is my response.
type RequestVoteReply =
    {
        /// The term I think it is.
        VoterTerm : int<Term>
        /// Whether I am happy for you to become leader. (For example, if my term is greater than yours, then you're
        /// out of date and I won't vote for you.)
        VoteGranted : bool
    }

/// I am starting an election. Everyone, please vote.
type RequestVoteMessage =
    {
        CandidateTerm : int<Term>
        CandidateId : int<ServerId>
        CandidateLastLogEntry : int<LogIndex> * int<Term>
        ReplyChannel : RequestVoteReply -> unit
    }

/// I, a follower, acknowledge the leader's instruction to add an entry to my log.
type AppendEntriesReply =
    {
        FollowerTerm : int<Term>
        /// Reply with failure if the follower thinks the leader is out of date:
        /// that is, if the leader's believed term is smaller than the follower's,
        /// or if the leader's declared previous log entry doesn't exist on the follower.
        Success : bool
    }

type LogEntry =
    {
        Index : int<LogIndex>
        Term : int<Term>
    }

/// I am the leader. Followers, update your state as follows.
type AppendEntriesMessage<'a> =
    {
        /// This is what term I, the leader, think it is.
        LeaderTerm : int<Term>
        /// I am your leader! This is me! (so everyone knows where to send clients to)
        LeaderId : int<ServerId>
        /// The entry immediately preceding the entry I'm sending you, so you can tell if we've got out of sync.
        PrevLogEntry : LogEntry
        /// Followers, append this entry to your log. (Or, if None, this is just a heartbeat.)
        /// It was determined at the given term - recall that we might need to bring a restarted node up to speed
        /// with what happened during terms that took place while it was down.
        NewEntry : ('a * int<Term>) option
        LeaderCommitIndex : int<LogIndex>
        ReplyChannel : AppendEntriesReply -> unit
    }

type Message<'a> =
    | AppendEntries of AppendEntriesMessage<'a>
    | RequestVote of RequestVoteMessage

    member this.Term =
        match this with
        | AppendEntries m -> m.LeaderTerm
        | RequestVote m -> m.CandidateTerm

type ServerSpecialisation =
    | Leader of LeaderState
    | Follower
    | Candidate

type ServerAction<'a> =
    | BeginElection
    | Receive of Message<'a>
    | Sync of AsyncReplyChannel<unit>

type Server<'a>
    (
        clusterSize : int,
        persistentState : IPersistentState<'a>,
        messageChannel : int<ServerId> -> Message<'a> -> unit
    )
     as this=
    let mutable volatileState = VolatileState.New
    let mutable currentType = Follower

    let processMessage (message : Message<'a>) : unit =
        // First, see if this message comes from a future term.
        // (This is `UpdateTerm` from the TLA+.)
        if message.Term > persistentState.CurrentTerm then
            // We're definitely out of date. Switch to follower mode.
            currentType <- Follower
            persistentState.AdvanceToTerm message.Term

        match message with
        | RequestVote message ->
            // This was guaranteed above.
            assert (message.CandidateTerm <= persistentState.CurrentTerm)

            // The following clauses define either condition under which we accept that the candidate has more data
            // than we do, and so could be a more suitable leader than us.

            // TODO collapse these clauses, it'll be much neater

            let messageSupersedesMe =
                // Is the candidate advertising a later term than our last-persisted write was made at?
                // (That would mean it's far in the future of us.)
                match persistentState.GetLastLogEntry () with
                | Some (_, ourLastTerm) -> snd message.CandidateLastLogEntry > ourLastTerm
                | None ->
                    // We have persisted no history at all!
                    true

            let messageExtendsMe =
                // Do we agree what the current term is, is the candidate advertising a more advanced log than us?
                match persistentState.GetLastLogEntry () with
                | Some (_, ourLastTerm) ->
                    snd message.CandidateLastLogEntry = ourLastTerm
                    && fst message.CandidateLastLogEntry >= persistentState.CurrentLogIndex
                | None ->
                    // We've persisted no history; the candidate needs to also be at the start of history,
                    // or else we'd have already considered them in the `messageSupersedesMe` check.
                    snd message.CandidateLastLogEntry = 0<Term>

            // (This is the `logOk` of the paper. It's true iff the candidate's declaration is ahead of us.)
            let candidateIsAhead = messageSupersedesMe || messageExtendsMe

            // But just because the candidate is ahead of us, doesn't mean we can vote for them.
            // We can only vote for one candidate per election.
            // (We can't rely on our own VotedFor property, because that may have been in a previous election.)

            let shouldVoteFor =
                if message.CandidateTerm = persistentState.CurrentTerm && candidateIsAhead then
                    // We agree on which election we're taking part in, and moreover we agree that the candidate is
                    // suitable.
                    match persistentState.VotedFor with
                    | None ->
                        // We haven't voted in this election before.
                        true
                    | Some i when i = message.CandidateId ->
                        // We have voted in this election before, but we voted for the same candidate. It's fine to
                        // repeat our agreement.
                        true
                    | Some _ ->
                        // We have voted in this election before, but for a different candidate. We can't vote for more
                        // than one candidate.
                        false
                else
                    // We think the candidate is behind us (either because its term is less than ours, or because
                    // its last committed write is not later than ours), so is an unsuitable leader.
                    false

            if shouldVoteFor then
                // We must persist our voting state before sending a reply, in case we die before
                // getting a chance to persist.
                // (Better for us to wrongly think we've voted than to wrongly think we've yet to vote. In the worst
                // case we just end up not participating in an election.)

                persistentState.Vote message.CandidateId

                {
                    VoterTerm = persistentState.CurrentTerm
                    VoteGranted = true
                }
                |> message.ReplyChannel

        | AppendEntries message ->
            // This was guaranteed above.
            assert (message.LeaderTerm <= persistentState.CurrentTerm)

            if message.LeaderTerm < persistentState.CurrentTerm then
                // Reject the request: the "leader" is actually outdated, it was only a leader in the past.
                {
                    FollowerTerm = persistentState.CurrentTerm
                    Success = false
                }
                |> message.ReplyChannel

            else

            // This was guaranteed immediately above: we agree that the message is being sent by the current leader.
            assert (message.LeaderTerm = persistentState.CurrentTerm)

            let heartbeat (message : AppendEntriesMessage<'a>) : unit =
                // Just a heartbeat; no change to our log is required.
                volatileState <-
                    { volatileState with
                        CommitIndex = message.LeaderCommitIndex
                    }

                {
                    Success = true
                    FollowerTerm = persistentState.CurrentTerm
                }
                |> message.ReplyChannel

            let acceptRequest () : unit =
                assert (currentType = Follower)

                match message.NewEntry with
                | None -> heartbeat message

                | Some (toInsert, toInsertTerm) ->

                let desiredLogInsertionPosition = message.PrevLogEntry.Index + 1<LogIndex>

                match persistentState.GetLogEntry desiredLogInsertionPosition with
                | Some (_, existingTerm) when toInsertTerm = existingTerm ->
                    // This is already persisted. Moreover, the value that we persisted came from the term we're
                    // currently processing, so in particular came from the same leader and hence won't conflict
                    // with what we received from that leader just now.
                    heartbeat message
                | Some _ ->
                    // The leader's message conflicts with what we persisted. Defer to the leader.
                    persistentState.TruncateLog (desiredLogInsertionPosition - 1<LogIndex>)
                    persistentState.AppendToLog toInsert toInsertTerm

                    {
                        Success = true
                        FollowerTerm = persistentState.CurrentTerm
                    }
                    |> message.ReplyChannel

                | None ->
                    // The leader knows what we've committed, so it won't try and give us anything further than
                    // the element immediately past our persisted log.
                    // TODO - why can't this be -1?
                    assert (desiredLogInsertionPosition = 1<LogIndex> + persistentState.CurrentLogIndex)
                    // The leader's message is after our log. Append.
                    persistentState.AppendToLog toInsert toInsertTerm

                    {
                        Success = true
                        FollowerTerm = persistentState.CurrentTerm
                    }
                    |> message.ReplyChannel

            let logIsConsistent (message : AppendEntriesMessage<'a>) : bool =
                if message.PrevLogEntry.Index = 0<LogIndex> then
                    // The leader advertises that they have no committed history, so certainly it's consistent with
                    // us.
                    true
                else

                match persistentState.GetLogEntry message.PrevLogEntry.Index with
                | None ->
                    // The leader's advertised commit is ahead of our history.
                    false
                | Some (_, ourTermForThisEntry) ->
                    // The leader's advertised commit is in our history; do we agree with it?
                    ourTermForThisEntry = message.PrevLogEntry.Term

            match currentType with
            | Leader _ ->
                [
                    "Violation of invariant."
                    "This is a logic error that cannot happen unless there is a bug in this Raft implementation."
                    sprintf
                        "There are two leaders in the current term %i, or else the leader has heartbeated itself."
                        message.LeaderTerm
                ]
                |> String.concat "\n"
                |> failwithf "%s"

            | Follower ->
                if not (logIsConsistent message) then
                    // Reject the request, it's inconsistent with our history.
                    {
                        FollowerTerm = persistentState.CurrentTerm
                        Success = false
                    }
                    |> message.ReplyChannel

                else
                    acceptRequest ()

            | Candidate ->
                // We've already verified that the message was sent from a leader in the current term, so we have
                // lost the election.
                currentType <- Follower
                // TODO: why does this assertion hold?
                assert (logIsConsistent message)
                acceptRequest ()

    let mailbox =
        let rec loop (mailbox : MailboxProcessor<_>) =
            async {
                let! m = mailbox.Receive ()

                match m with
                | ServerAction.BeginElection -> return failwith "not yet implemented"
                | ServerAction.Receive m -> return processMessage m
                | ServerAction.Sync reply -> reply.Reply ()

                return! loop mailbox
            }

        loop |> MailboxProcessor.Start

    member this.TriggerTimeout () = mailbox.Post ServerAction.BeginElection

    member this.Message (m : Message<'a>) = mailbox.Post (ServerAction.Receive m)

    member this.Sync () = mailbox.PostAndReply ServerAction.Sync

// {
//     ClusterSize : int
//     mutable VolatileState : VolatileState
//     PersistentState : IPersistentState<'a>
//     mutable Type : ServerSpecialisation
//     TriggerTimeout : unit -> unit
//     Mailbox : MailboxProcessor<ServerAction<'a>>
// }

type Cluster<'a> =
    internal
        {
            Servers : Server<'a> array
            SendMessage : int<ServerId> -> Message<'a> -> unit
        }

[<RequireQualifiedAccess>]
module InMemoryCluster =

    [<RequiresExplicitTypeArguments>]
    let make<'a> (immediateFlush : bool) (count : int) : Cluster<'a> =
        let servers = Array.zeroCreate<Server<'a>> count

        let locker = obj ()

        let messageChannel (serverId : int<ServerId>) (message : Message<'a>) : unit =
            lock locker (fun () -> servers.[serverId / 1<ServerId>].Message message)

        for s in 0 .. servers.Length - 1 do
            servers.[s] <- Server (count, InMemoryPersistentState (), messageChannel)

        {
            Servers = servers
            SendMessage =
                if immediateFlush then
                    fun i m ->
                        servers.[i / 1<ServerId>].Message m
                        servers.[i / 1<ServerId>].Sync ()
                else
                    fun i -> servers.[i / 1<ServerId>].Message
        }
