namespace Raft

open System.Collections.Generic

type Cluster<'a> =
    internal
        {
            Servers : Server<'a> array
            SendMessageDirectly : int<ServerId> -> Message<'a> -> unit
        }

    member this.SendMessage (i : int<ServerId>) (m : Message<'a>) : unit = this.SendMessageDirectly i m

    member this.InactivityTimeout (i : int<ServerId>) : unit =
        this.Servers.[i / 1<ServerId>].TriggerInactivityTimeout ()
        this.Servers.[i / 1<ServerId>].Sync ()

    member this.HeartbeatTimeout (i : int<ServerId>) : unit =
        this.Servers.[i / 1<ServerId>].TriggerHeartbeatTimeout ()
        this.Servers.[i / 1<ServerId>].Sync ()

    member this.Status (i : int<ServerId>) : ServerStatus = this.Servers.[i / 1<ServerId>].State

    member this.GetCurrentInternalState (i : int<ServerId>) : ServerInternalState<'a> Async =
        this.Servers.[i / 1<ServerId>].GetCurrentInternalState ()

    member this.ClusterSize : int = this.Servers.Length

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

    member this.DropMessage (i : int<ServerId>) (id : int) : unit =
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

type NetworkAction<'a> =
    | InactivityTimeout of int<ServerId>
    | NetworkMessage of int<ServerId> * int
    | DropMessage of int<ServerId> * int
    | ClientRequest of int<ServerId> * 'a * (ClientReply -> unit)
    | Heartbeat of int<ServerId>

[<RequireQualifiedAccess>]
module NetworkAction =

    let perform<'a> (cluster : Cluster<'a>) (network : Network<'a>) (action : NetworkAction<'a>) : unit =
        match action with
        | InactivityTimeout serverId -> cluster.InactivityTimeout serverId
        | Heartbeat serverId -> cluster.HeartbeatTimeout serverId
        | DropMessage (serverId, messageId) -> network.DropMessage serverId messageId
        | NetworkMessage (serverId, messageId) ->
            network.InboundMessage serverId messageId |> cluster.SendMessage serverId
            network.DropMessage serverId messageId
        | ClientRequest (server, request, replyChannel) ->
            Message.ClientRequest (request, replyChannel) |> cluster.SendMessage server
