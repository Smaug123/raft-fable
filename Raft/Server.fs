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
        MatchIndex : int<LogIndex> array
    }

    static member New (clusterSize : int) (currentIndex : int<LogIndex>) : LeaderState =
        {
            NextIndex = Array.create clusterSize (currentIndex + 1<LogIndex>)
            MatchIndex = Array.zeroCreate clusterSize
        }

/// You asked me to vote for you to become leader. Here is my response.
type RequestVoteReply =
    {
        /// Me!
        Voter : int<ServerId>
        /// The term I think it is.
        VoterTerm : int<Term>
        /// Whether I am happy for you to become leader. (For example, if my term is greater than yours, then you're
        /// out of date and I won't vote for you.)
        VoteGranted : bool
        /// The candidate I'm voting for or against.
        Candidate : int<ServerId>
    }

    override this.ToString () =
        let decision = if this.VoteGranted then "in favour of" else "against"
        sprintf "Server %i voting %s %i in term %i" this.Voter decision this.Candidate this.VoterTerm

/// I am starting an election. Everyone, please vote.
type RequestVoteMessage =
    {
        CandidateTerm : int<Term>
        CandidateId : int<ServerId>
        CandidateLastLogEntry : LogEntry option
        ReplyChannel : RequestVoteReply -> unit
    }

    override this.ToString () =
        sprintf "Vote request: %i in term %i" this.CandidateId this.CandidateTerm

/// I, a follower, acknowledge the leader's instruction to add an entry to my log.
type AppendEntriesReply =
    {
        /// Me, the follower who is replying
        Follower : int<ServerId>
        /// The term I, the follower, think it is
        FollowerTerm : int<Term>
        /// Reply with None if the follower thinks the leader is out of date:
        /// that is, if the leader's believed term is smaller than the follower's,
        /// or if the leader's declared previous log entry doesn't exist on the follower.
        /// If instead we accepted the update, this is the current head of the follower's log
        /// after accepting the update.
        Success : int<LogIndex> option
    }

    override this.ToString () =
        let description =
            match this.Success with
            | Some index -> sprintf "successfully applied leader entry, log length %i" index
            | None -> "did not apply leader entry"

        sprintf "Follower %i %s" this.FollowerTerm description

/// I am the leader. Followers, update your state as follows.
type AppendEntriesMessage<'a> =
    {
        /// This is what term I, the leader, think it is.
        LeaderTerm : int<Term>
        /// I am your leader! This is me! (so everyone knows where to send clients to)
        LeaderId : int<ServerId>
        /// The entry immediately preceding the entry I'm sending you, so you can tell if we've got out of sync.
        PrevLogEntry : LogEntry option
        /// Followers, append this entry to your log. (Or, if None, this is just a heartbeat.)
        /// It was determined at the given term - recall that we might need to bring a restarted node up to speed
        /// with what happened during terms that took place while it was down.
        NewEntry : ('a * int<Term>) option
        LeaderCommitIndex : int<LogIndex>
        ReplyChannel : AppendEntriesReply -> unit
    }

    override this.ToString () =
        let description =
            match this.NewEntry with
            | None -> "Heartbeat"
            | Some (entry, term) -> sprintf "Append %+A (term %i)" entry term

        sprintf
            "%s (leader %i at term %i whose commit index is %i)"
            description
            this.LeaderId
            this.LeaderTerm
            this.LeaderCommitIndex

type Instruction<'a> =
    | AppendEntries of AppendEntriesMessage<'a>
    | RequestVote of RequestVoteMessage

    override this.ToString () =
        match this with
        | RequestVote v -> v.ToString ()
        | AppendEntries a -> a.ToString ()

    member this.Term =
        match this with
        | AppendEntries m -> m.LeaderTerm
        | RequestVote m -> m.CandidateTerm

type Reply =
    | RequestVoteReply of RequestVoteReply
    | AppendEntriesReply of AppendEntriesReply

    override this.ToString () =
        match this with
        | RequestVoteReply v -> v.ToString ()
        | AppendEntriesReply r -> r.ToString ()

type Message<'a> =
    | Instruction of Instruction<'a>
    | Reply of Reply

    override this.ToString () =
        match this with
        | Instruction i -> i.ToString ()
        | Reply r -> r.ToString ()

type private CandidateState =
    {
        /// For each voter, the vote I received from them.
        Votes : bool option array
    }

    static member New (count : int) (self : int<ServerId>) =
        let votes = Array.zeroCreate<_> count
        votes.[self / 1<ServerId>] <- Some true

        {
            Votes = votes
        }

[<RequireQualifiedAccess>]
type private ServerSpecialisation =
    | Leader of LeaderState
    | Follower
    | Candidate of CandidateState

type ServerStatus =
    | Leader of int<Term>
    | Follower
    | Candidate of int<Term>

    override this.ToString () =
        match this with
        | Leader term -> sprintf "Leader in term %i" term
        | Candidate term -> sprintf "Candidate in term %i" term
        | Follower -> "Follower"

type private ServerAction<'a> =
    | BeginElection
    | EmitHeartbeat
    | Receive of Message<'a>
    | Sync of AsyncReplyChannel<unit>

[<RequireQualifiedAccess>]
module internal ServerUtils =

    /// Return the maximum log index which a quorum has committed.
    /// Recall that 0 means "nothing committed".
    let maxLogAQuorumHasCommitted (matchIndex : int<LogIndex>[]) : int<LogIndex> =
        let numberWhoCommittedIndex = matchIndex |> Array.countBy id

        numberWhoCommittedIndex |> Array.sortInPlaceBy fst

        let rec go (numberCounted : int) (result : int<LogIndex>) (i : int) =
            if i < 0 then
                result
            else
                let numberCounted = numberCounted + snd numberWhoCommittedIndex.[i]

                if numberCounted > matchIndex.Length / 2 then
                    fst numberWhoCommittedIndex.[i]
                else
                    go numberCounted (fst numberWhoCommittedIndex.[i]) (i - 1)

        go 0 0<LogIndex> (numberWhoCommittedIndex.Length - 1)

type Server<'a>
    (
        clusterSize : int,
        me : int<ServerId>,
        persistentState : IPersistentState<'a>,
        messageChannel : int<ServerId> -> Message<'a> -> unit
    )
    =
    let mutable volatileState = VolatileState.New
    let mutable currentType = ServerSpecialisation.Follower

    let processMessage (message : Instruction<'a>) : unit =
        // First, see if this message comes from a future term.
        // (This is `UpdateTerm` from the TLA+.)
        if message.Term > persistentState.CurrentTerm then
            // We're definitely out of date. Switch to follower mode.
            currentType <- ServerSpecialisation.Follower
            persistentState.AdvanceToTerm message.Term
        // TODO - `DropStaleResponse` suggests we should do this
        //elif message.Term < persistentState.CurrentTerm then
        //    // Drop the message, it's old.
        //    ()
        //else

        match message with
        | RequestVote message ->
            // This was guaranteed above.
            assert (message.CandidateTerm <= persistentState.CurrentTerm)

            // The following clause defines the condition under which we accept that the candidate has more data
            // than we do, and so could be a more suitable leader than us.

            // (This is the `logOk` of the paper.)
            let candidateSupersedesMe =
                match persistentState.GetLastLogEntry (), message.CandidateLastLogEntry with
                | Some (_, ourLastEntry), Some candidateLastLogEntry ->
                    // The candidate wins if:
                    // * it's from so far in the future that an election has happened which we haven't heard about; or
                    // * it's from the same term as us, but it's logged more than we have.
                    candidateLastLogEntry.Term > ourLastEntry.Term
                    || (candidateLastLogEntry.Term = ourLastEntry.Term
                        && candidateLastLogEntry.Index >= persistentState.CurrentLogIndex)
                | Some _, None ->
                    // We've logged something, and they have not. We're ahead, and won't vote for them.
                    false
                | None, _ ->
                    // We have persisted no history at all.
                    // They take precedence - either they've logged something (in which case they must be ahead of us),
                    // or they haven't logged anything (in which case we'll defer to them for beating us to the
                    // first election).
                    true

            // But just because the candidate is ahead of us, doesn't mean we can vote for them.
            // We can only vote for one candidate per election.
            // (We can't rely on our own VotedFor property, because that may have been in a previous election.)

            let shouldVoteFor =
                if message.CandidateTerm = persistentState.CurrentTerm && candidateSupersedesMe then
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
                    Voter = me
                    VoterTerm = persistentState.CurrentTerm
                    VoteGranted = true
                    Candidate = message.CandidateId
                }
                |> message.ReplyChannel

        | AppendEntries message ->
            // This was guaranteed above.
            assert (message.LeaderTerm <= persistentState.CurrentTerm)

            if message.LeaderTerm < persistentState.CurrentTerm then
                // Reject the request: the "leader" is actually outdated, it was only a leader in the past.
                {
                    FollowerTerm = persistentState.CurrentTerm
                    Success = None
                    Follower = me
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
                    Success = Some persistentState.CurrentLogIndex
                    FollowerTerm = persistentState.CurrentTerm
                    Follower = me
                }
                |> message.ReplyChannel

            let acceptRequest () : unit =
                assert (currentType = ServerSpecialisation.Follower)

                match message.NewEntry with
                | None -> heartbeat message

                | Some (toInsert, toInsertTerm) ->

                let desiredLogInsertionPosition =
                    match message.PrevLogEntry with
                    | None -> 1<LogIndex>
                    | Some entry -> entry.Index + 1<LogIndex>

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
                        Success = Some desiredLogInsertionPosition
                        FollowerTerm = persistentState.CurrentTerm
                        Follower = me
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
                        Success = Some desiredLogInsertionPosition
                        FollowerTerm = persistentState.CurrentTerm
                        Follower = me
                    }
                    |> message.ReplyChannel

            let logIsConsistent (message : AppendEntriesMessage<'a>) : bool =
                match message.PrevLogEntry with
                | None ->
                    // The leader advertises that they have no committed history, so certainly it's consistent with
                    // us.
                    true
                | Some entry ->

                match persistentState.GetLogEntry entry.Index with
                | None ->
                    // The leader's advertised commit is ahead of our history.
                    false
                | Some (_, ourTermForThisEntry) ->
                    // The leader's advertised commit is in our history; do we agree with it?
                    ourTermForThisEntry = entry.Term

            match currentType with
            | ServerSpecialisation.Leader _ ->
                [
                    "Violation of invariant."
                    "This is a logic error that cannot happen unless there is a bug in this Raft implementation."
                    sprintf
                        "There are two leaders in the current term %i, or else the leader has heartbeated itself."
                        message.LeaderTerm
                ]
                |> String.concat "\n"
                |> failwithf "%s"

            | ServerSpecialisation.Follower ->
                if not (logIsConsistent message) then
                    // Reject the request, it's inconsistent with our history.
                    {
                        FollowerTerm = persistentState.CurrentTerm
                        Success = None
                        Follower = me
                    }
                    |> message.ReplyChannel

                else
                    acceptRequest ()

            | ServerSpecialisation.Candidate _ ->
                // We've already verified that the message was sent from a leader in the current term, so we have
                // lost the election.
                currentType <- ServerSpecialisation.Follower
                // TODO: why does this assertion hold?
                assert (logIsConsistent message)
                acceptRequest ()

    let divideByTwoRoundingUp (n : int) =
        if n % 2 = 0 then n / 2 else (n / 2) + 1

    let emitHeartbeat () =
        match currentType with
        | ServerSpecialisation.Candidate _
        | ServerSpecialisation.Follower -> ()
        | ServerSpecialisation.Leader _ ->
            for i in 0 .. clusterSize - 1 do
                if i * 1<ServerId> <> me then
                    {
                        LeaderTerm = persistentState.CurrentTerm
                        LeaderId = me
                        PrevLogEntry = persistentState.GetLastLogEntry () |> Option.map snd
                        NewEntry = None
                        LeaderCommitIndex = volatileState.CommitIndex
                        ReplyChannel =
                            fun reply -> messageChannel me (reply |> Reply.AppendEntriesReply |> Message.Reply)
                    }
                    |> Instruction.AppendEntries
                    |> Message.Instruction
                    |> messageChannel (i * 1<ServerId>)

    let processReply (r : Reply) : unit =
        match r with
        | AppendEntriesReply appendEntriesReply ->
            match currentType with
            | ServerSpecialisation.Candidate _
            | ServerSpecialisation.Follower -> ()
            | ServerSpecialisation.Leader leaderState ->

            if appendEntriesReply.FollowerTerm = persistentState.CurrentTerm then
                match appendEntriesReply.Success with
                | Some matchIndex ->
                    leaderState.MatchIndex.[appendEntriesReply.Follower / 1<ServerId>] <- matchIndex
                    leaderState.NextIndex.[appendEntriesReply.Follower / 1<ServerId>] <- matchIndex + 1<LogIndex>
                | None ->
                    leaderState.NextIndex.[appendEntriesReply.Follower / 1<ServerId>] <-
                        max
                            (leaderState.NextIndex.[appendEntriesReply.Follower / 1<ServerId>] - 1<LogIndex>)
                            1<LogIndex>
                // Note that the decision to process this *here* means the algorithm doesn't work in clusters of
                // only one node, because then there will never be any AppendEntries replies.
                let maxLogAQuorumHasCommitted =
                    ServerUtils.maxLogAQuorumHasCommitted leaderState.MatchIndex

                if maxLogAQuorumHasCommitted > 0<LogIndex> then
                    let ourLogTerm =
                        match persistentState.GetLogEntry maxLogAQuorumHasCommitted with
                        | None ->
                            failwithf
                                "Invariant violated. The leader has not logged an entry (%i) which it knows a quorum has logged."
                                maxLogAQuorumHasCommitted
                        | Some (_, term) -> term

                    if ourLogTerm = persistentState.CurrentTerm then
                        volatileState <-
                            { volatileState with
                                CommitIndex = maxLogAQuorumHasCommitted
                            }

        | RequestVoteReply requestVoteReply ->
            match currentType with
            | ServerSpecialisation.Leader _
            | ServerSpecialisation.Follower ->
                // We're not expecting any votes; drop them.
                ()
            | ServerSpecialisation.Candidate state ->

            if requestVoteReply.VoterTerm = persistentState.CurrentTerm then
                state.Votes.[requestVoteReply.Voter / 1<ServerId>] <- Some requestVoteReply.VoteGranted

                // Inefficient, but :shrug:
                if
                    Array.sumBy
                        (function
                        | Some true -> 1
                        | _ -> 0)
                        state.Votes > clusterSize / 2
                then
                    // Become the leader!
                    currentType <-
                        LeaderState.New clusterSize persistentState.CurrentLogIndex
                        |> ServerSpecialisation.Leader

                    emitHeartbeat ()

    let mailbox =
        let rec loop (mailbox : MailboxProcessor<_>) =
            async {
                let! m = mailbox.Receive ()
                //let toPrint = sprintf "Processing message in server %i: %+A" me m
                //System.Console.WriteLine toPrint

                match m with
                | ServerAction.EmitHeartbeat -> emitHeartbeat ()
                | ServerAction.BeginElection ->
                    match currentType with
                    | ServerSpecialisation.Leader _ -> ()
                    | ServerSpecialisation.Candidate _
                    | ServerSpecialisation.Follower ->

                    // Start the election!
                    currentType <- ServerSpecialisation.Candidate (CandidateState.New clusterSize me)
                    persistentState.IncrementTerm ()
                    persistentState.Vote me

                    for i in 0 .. clusterSize - 1 do
                        if i * 1<ServerId> <> me then
                            {
                                CandidateTerm = persistentState.CurrentTerm
                                CandidateId = me
                                CandidateLastLogEntry = persistentState.GetLastLogEntry () |> Option.map snd
                                ReplyChannel = fun reply -> messageChannel me (RequestVoteReply reply |> Message.Reply)
                            }
                            |> Instruction.RequestVote
                            |> Message.Instruction
                            |> messageChannel (i * 1<ServerId>)
                | ServerAction.Receive (Instruction m) -> processMessage m
                | ServerAction.Receive (Reply r) -> processReply r
                | ServerAction.Sync replyChannel -> replyChannel.Reply ()

                return! loop mailbox
            }

        let mailbox = loop |> MailboxProcessor.Start
        mailbox.Error.Add raise
        mailbox

    member this.TriggerInactivityTimeout () = mailbox.Post ServerAction.BeginElection
    member this.TriggerHeartbeatTimeout () = mailbox.Post ServerAction.EmitHeartbeat

    member this.Message (m : Message<'a>) = mailbox.Post (ServerAction.Receive m)

    member this.Sync () = mailbox.PostAndReply ServerAction.Sync

    member this.State =
        match currentType with
        | ServerSpecialisation.Leader _ -> ServerStatus.Leader persistentState.CurrentTerm
        | ServerSpecialisation.Candidate _ -> ServerStatus.Candidate persistentState.CurrentTerm
        | ServerSpecialisation.Follower -> ServerStatus.Follower
