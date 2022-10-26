namespace Raft

open System.Collections.Generic

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
        CandidateLastLogEntry : int<LogIndex> * int<Term>
        ReplyChannel : RequestVoteReply -> unit
    }

    override this.ToString () =
        sprintf "Vote request: %i in term %i" this.CandidateId this.CandidateTerm

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

    override this.ToString () =
        sprintf "Log entry %i at subjective term %i" this.Index this.Term

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

    override this.ToString () =
        match this with
        | RequestVoteReply v -> v.ToString ()

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
    | Leader
    | Follower
    | Candidate

type private ServerAction<'a> =
    | BeginElection
    | Receive of Message<'a>
    | Sync of AsyncReplyChannel<unit>

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
                | Some (_, (_, ourLastTerm)) -> snd message.CandidateLastLogEntry > ourLastTerm
                | None ->
                    // We have persisted no history at all!
                    true

            let messageExtendsMe =
                // Do we agree what the current term is, is the candidate advertising a more advanced log than us?
                match persistentState.GetLastLogEntry () with
                | Some (_, (_, ourLastTerm)) ->
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
                assert (currentType = ServerSpecialisation.Follower)

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
                        Success = false
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

    let mailbox =
        let rec loop (mailbox : MailboxProcessor<_>) =
            async {
                let! m = mailbox.Receive ()
                //let toPrint = sprintf "Processing message in server %i: %+A" me m
                //System.Console.WriteLine toPrint

                match m with
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
                                CandidateLastLogEntry =
                                    match persistentState.GetLastLogEntry () with
                                    | Some (index, (_, term)) -> (index, term)
                                    | None ->
                                        // TODO this is almost certainly not right
                                        (0<LogIndex>, 0<Term>)
                                ReplyChannel =
                                    // TODO this is bypassing the network - stop it!
                                    fun reply -> messageChannel me (RequestVoteReply reply |> Message.Reply)
                            }
                            |> Instruction.RequestVote
                            |> Message.Instruction
                            |> messageChannel (i * 1<ServerId>)
                | ServerAction.Receive (Instruction m) -> return processMessage m
                | ServerAction.Sync reply -> reply.Reply ()
                | ServerAction.Receive (Reply (RequestVoteReply requestVoteReply)) ->
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

                return! loop mailbox
            }

        let mailbox = loop |> MailboxProcessor.Start
        mailbox.Error.Add raise
        mailbox

    member this.TriggerTimeout () = mailbox.Post ServerAction.BeginElection

    member this.Message (m : Message<'a>) = mailbox.Post (ServerAction.Receive m)

    member this.Sync () = mailbox.PostAndReply ServerAction.Sync

    member this.State =
        match currentType with
        | ServerSpecialisation.Leader _ -> ServerStatus.Leader
        | ServerSpecialisation.Candidate _ -> ServerStatus.Candidate
        | ServerSpecialisation.Follower -> ServerStatus.Follower

type Cluster<'a> =
    internal
        {
            Servers : Server<'a> array
            SendMessageDirectly : int<ServerId> -> Message<'a> -> unit
        }

    member this.SendMessage (i : int<ServerId>) (m : Message<'a>) : unit = this.SendMessageDirectly i m

    member this.Timeout (i : int<ServerId>) : unit =
        this.Servers.[i / 1<ServerId>].TriggerTimeout ()
        this.Servers.[i / 1<ServerId>].Sync ()

type Network<'a> =
    internal
        {
            /// CompleteMessageHistory.[i] is the collection of all messages
            /// ever sent to server `i`.
            CompleteMessageHistory : ResizeArray<Message<'a>>[]
            MessagesDelivered : HashSet<int>[]
        }

    static member Make (clusterSize : int) =
        {
            CompleteMessageHistory = Array.init clusterSize (fun _ -> ResizeArray ())
            MessagesDelivered = Array.init clusterSize (fun _ -> HashSet ())
        }

    member this.AllInboundMessages (i : int<ServerId>) : Message<'a> list =
        this.CompleteMessageHistory.[i / 1<ServerId>] |> List.ofSeq

    member this.InboundMessage (i : int<ServerId>) (id : int) : Message<'a> =
        this.CompleteMessageHistory.[i / 1<ServerId>].[id]

    member this.DropMessage (i : int<ServerId>) (id : int) =
        this.MessagesDelivered.[i / 1<ServerId>].Add id |> ignore

    member this.UndeliveredMessages (i : int<ServerId>) : (int * Message<'a>) list =
        this.CompleteMessageHistory.[i / 1<ServerId>]
        |> Seq.indexed
        |> Seq.filter (fun (count, _) -> this.MessagesDelivered.[i / 1<ServerId>].Contains count |> not)
        |> List.ofSeq

    member this.AllUndeliveredMessages () : ((int * Message<'a>) list) list =
        List.init this.CompleteMessageHistory.Length (fun i -> this.UndeliveredMessages (i * 1<ServerId>))

    member this.ClusterSize = this.CompleteMessageHistory.Length

[<RequireQualifiedAccess>]
module InMemoryCluster =

    [<RequiresExplicitTypeArguments>]
    let make<'a> (count : int) : Cluster<'a> * Network<'a> =
        let servers = Array.zeroCreate<Server<'a>> count

        let network = Network<int>.Make count

        let messageChannelHold (serverId : int<ServerId>) (message : Message<'a>) : unit =
            let arr = network.CompleteMessageHistory.[serverId / 1<ServerId>]
            lock arr (fun () -> arr.Add message)

        for s in 0 .. servers.Length - 1 do
            servers.[s] <- Server (count, s * 1<ServerId>, InMemoryPersistentState (), messageChannelHold)

        let cluster =
            {
                Servers = servers
                SendMessageDirectly =
                    fun i m ->
                        servers.[i / 1<ServerId>].Message m
                        servers.[i / 1<ServerId>].Sync ()
            }

        cluster, network
