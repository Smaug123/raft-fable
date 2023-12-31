namespace RaftFable

open System.Collections.Generic
open Raft

type ClusterState<'a> =
    {
        ClusterSize : int
        InternalState : ServerInternalState<'a> array
        Statuses : ServerStatus array
        AllMessages : Message<'a> list array
        UndeliveredMessages : (int * Message<'a>) list array
    }

type UserPreferences<'a> =
    {
        LeaderUnderConsideration : int<ServerId>
        ShowConsumedMessages : bool
        ActionHistory : NetworkAction<'a> list
    }

type UiBackingState<'a> =
    {
        ClusterState : ClusterState<'a>
        UserPreferences : UserPreferences<'a>
        /// TODO - make this be IReadOnlySet, not HashSet
        Clients : IReadOnlyDictionary<int<ClientId>, int<ClientSequence> HashSet>
    }

type ClientDataSection =
    {
        ClientDataField : Browser.Types.HTMLInputElement
        ClientDataServerField : Browser.Types.HTMLInputElement
        ClientIdField : Browser.Types.HTMLInputElement
        ClientSequenceField : Browser.Types.HTMLInputElement
    }

type UiElements =
    {
        Document : Browser.Types.Document
        ServerStatusTable : Browser.Types.HTMLTableElement
        LogArea : Browser.Types.HTMLTableElement
        MessageQueueArea : Browser.Types.HTMLTableElement
        LeaderStateTable : Browser.Types.HTMLTableElement
        TimeoutField : Browser.Types.HTMLInputElement
        HeartbeatField : Browser.Types.HTMLInputElement
        SelectedLeaderId : Browser.Types.HTMLInputElement
        ShowConsumedMessages : Browser.Types.HTMLInputElement
        ActionHistoryList : Browser.Types.HTMLTextAreaElement
        ClientsList : Browser.Types.HTMLTableElement
        ClientData : ClientDataSection
        ClientCreateServer : Browser.Types.HTMLInputElement
    }

type RequiresPopulation =
    {
        ServerStatusNodes : Browser.Types.HTMLTableCellElement array
    }

[<RequireQualifiedAccess>]
module Ui =

    let initialise<'a> (document : Browser.Types.Document) : UiElements =
        let serverStatuses =
            document.querySelector ".server-statuses" :?> Browser.Types.HTMLTableElement

        let logArea = document.querySelector ".log-area" :?> Browser.Types.HTMLTableElement

        let messageQueueArea =
            document.querySelector ".button-area" :?> Browser.Types.HTMLTableElement

        let leaderStateTable =
            document.querySelector ".leader-state-table" :?> Browser.Types.HTMLTableElement

        let timeoutField =
            document.querySelector ".timeout-text" :?> Browser.Types.HTMLInputElement

        let clientDataServerField =
            document.querySelector ".client-server-selection" :?> Browser.Types.HTMLInputElement

        let heartbeatField =
            document.querySelector ".heartbeat-text" :?> Browser.Types.HTMLInputElement

        let clientDataField =
            document.querySelector ".client-data" :?> Browser.Types.HTMLInputElement

        let clientSequenceField =
            document.querySelector ".client-sequence" :?> Browser.Types.HTMLInputElement

        let clientIdField =
            document.querySelector ".client-id" :?> Browser.Types.HTMLInputElement

        let selectedLeaderId =
            document.querySelector ".leader-select" :?> Browser.Types.HTMLInputElement

        let showConsumed =
            document.querySelector ".show-consumed" :?> Browser.Types.HTMLInputElement

        let actionHistory =
            document.querySelector ".action-history" :?> Browser.Types.HTMLTextAreaElement

        let clientsList =
            document.querySelector ".clients" :?> Browser.Types.HTMLTableElement

        let clientCreateServer =
            document.querySelector ".create-client-server" :?> Browser.Types.HTMLInputElement

        let clientInfo =
            {
                ClientDataField = clientDataField
                ClientDataServerField = clientDataServerField
                ClientIdField = clientIdField
                ClientSequenceField = clientSequenceField
            }

        {
            Document = document
            ServerStatusTable = serverStatuses
            LogArea = logArea
            MessageQueueArea = messageQueueArea
            LeaderStateTable = leaderStateTable
            TimeoutField = timeoutField
            HeartbeatField = heartbeatField
            SelectedLeaderId = selectedLeaderId
            ShowConsumedMessages = showConsumed
            ActionHistoryList = actionHistory
            ClientsList = clientsList
            ClientData = clientInfo
            ClientCreateServer = clientCreateServer
        }

    let reset (clusterSize : int) (ui : UiElements) : RequiresPopulation =
        let document = ui.Document

        ui.ServerStatusTable.innerText <- ""
        ui.ServerStatusTable.border <- "1px"
        Table.createHeaderRow ui.Document [ "Server" ; "Status" ] ui.ServerStatusTable

        let serverStatusNodes =
            [|
                for i in 0 .. clusterSize - 1 do
                    let node = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement
                    let child = document.createElement "td" :?> Browser.Types.HTMLTableCellElement
                    let statusCell = document.createElement "td" :?> Browser.Types.HTMLTableCellElement
                    child.textContent <- sprintf "%i" i
                    node.appendChild child |> ignore
                    node.appendChild statusCell |> ignore
                    ui.ServerStatusTable.appendChild node |> ignore
                    yield statusCell
            |]

        ui.SelectedLeaderId.min <- "0"
        ui.SelectedLeaderId.max <- sprintf "%i" (clusterSize - 1)
        ui.SelectedLeaderId.defaultValue <- "0"
        ui.ClientData.ClientDataField.max <- "255"
        ui.ClientData.ClientDataField.min <- "0"
        ui.ClientData.ClientDataField.defaultValue <- "0"
        ui.ClientData.ClientDataServerField.max <- string<int> (clusterSize - 1)
        ui.ClientData.ClientDataServerField.min <- "0"
        ui.ClientData.ClientDataServerField.defaultValue <- "0"
        ui.ClientData.ClientIdField.min <- "0"
        ui.ClientData.ClientIdField.defaultValue <- "0"
        ui.ClientData.ClientSequenceField.min <- "0"
        ui.ClientData.ClientSequenceField.defaultValue <- "0"
        ui.ClientCreateServer.min <- "0"
        ui.ClientCreateServer.defaultValue <- "0"
        ui.ClientCreateServer.max <- string<int> (clusterSize - 1)
        ui.HeartbeatField.max <- string<int> (clusterSize - 1)
        ui.HeartbeatField.min <- "0"
        ui.HeartbeatField.defaultValue <- "0"
        ui.TimeoutField.max <- string<int> (clusterSize - 1)
        ui.TimeoutField.min <- "0"
        ui.TimeoutField.defaultValue <- "0"

        ui.LeaderStateTable.innerText <- ""

        ui.LeaderStateTable
        |> Table.createHeaderRow document ("" :: List.init clusterSize (sprintf "Server %i"))

        ui.MessageQueueArea.border <- "1px"
        ui.MessageQueueArea.innerText <- ""

        ui.MessageQueueArea
        |> Table.createHeaderRow document (List.init clusterSize (sprintf "Server %i"))

        ui.LogArea.border <- "1px"
        ui.LogArea.innerText <- ""

        ui.ShowConsumedMessages.defaultChecked <- false

        ui.ClientsList.border <- "1px"
        ui.ClientsList.innerText <- ""

        {
            ServerStatusNodes = serverStatusNodes
        }

    let renderPrefs<'a> (prefs : UserPreferences<'a>) (ui : UiElements) : unit =

        // Action list
        let actionList =
            prefs.ActionHistory |> Seq.map NetworkAction.toString |> String.concat "\n"

        ui.ActionHistoryList.value <- actionList

    let render<'a>
        (perform : NetworkAction<'a> -> Fable.Core.JS.Promise<unit>)
        (document : Browser.Types.Document)
        (ui : UiElements)
        (state : UiBackingState<'a>)
        : unit
        =
        let userPrefs = state.UserPreferences
        let requiresPopulation = reset state.ClusterState.ClusterSize ui

        let rows =
            List.init
                state.ClusterState.ClusterSize
                (fun i ->
                    let cell = document.createElement "th" :?> Browser.Types.HTMLTableCellElement
                    cell.textContent <- sprintf "Server %i" i
                    Table.createRow document [ Some cell ] ui.LogArea
                )

        rows
        |> List.iteri (fun i row ->
            let state = state.ClusterState.InternalState.[i]

            for logEntry in state.Log do
                let cell = document.createElement "td" :?> Browser.Types.HTMLTableCellElement

                match logEntry with
                | None -> cell.textContent <- "<none>"
                | Some (value, term) -> cell.textContent <- sprintf "%i: %O" term value

                row.appendChild cell |> ignore
        )

        for i in 0 .. state.ClusterState.ClusterSize - 1 do
            let status = state.ClusterState.Statuses.[i]
            requiresPopulation.ServerStatusNodes.[i].textContent <- status |> string<ServerStatus>

        // Network status
        let allButtons =
            [ 0 .. state.ClusterState.ClusterSize - 1 ]
            |> List.map (fun i ->
                if userPrefs.ShowConsumedMessages then
                    state.ClusterState.AllMessages.[i] |> List.indexed
                else
                    state.ClusterState.UndeliveredMessages.[i]
                |> List.map (fun (messageId, message) ->
                    Button.create
                        document
                        (sprintf "Server %i, message %i: %O" i messageId message)
                        (fun button ->
                            if not userPrefs.ShowConsumedMessages then
                                button.remove ()

                            NetworkMessage (i * 1<ServerId>, messageId) |> perform
                        )
                )
            )

        let maxQueueLength = allButtons |> Seq.map List.length |> Seq.max

        let allButtons' =
            allButtons
            |> List.map (fun l -> List.append (List.map Some l) (List.replicate (maxQueueLength - List.length l) None))
            |> List.transpose

        for row in allButtons' do
            Table.createRow document row ui.MessageQueueArea |> ignore

        // Leader status
        let leaderIdBox =
            document.querySelector ".leader-state" :?> Browser.Types.HTMLBlockElement

        leaderIdBox.innerText <- sprintf "%i" userPrefs.LeaderUnderConsideration

        let leaderState =
            state.ClusterState.InternalState.[userPrefs.LeaderUnderConsideration / 1<ServerId>]

        match leaderState.LeaderState with
        | None -> leaderIdBox.innerText <- sprintf "%i: not a leader" userPrefs.LeaderUnderConsideration
        | Some leaderState ->

            let knownStoredIndices =
                leaderState.MatchIndex
                |> Seq.mapi (fun target index ->
                    let target = target * 1<ServerId>

                    if target = userPrefs.LeaderUnderConsideration then
                        "(self)"
                    else
                        sprintf "%i" index
                )
                |> Seq.toList
                |> fun l -> "Log index known to be stored on each node" :: l
                |> List.map (fun text ->
                    let node = document.createElement "div"
                    node.innerText <- text
                    Some node
                )

            Table.createRow document knownStoredIndices ui.LeaderStateTable |> ignore

            let nextToSend =
                leaderState.ToSend
                |> Seq.mapi (fun target index ->
                    let target = target * 1<ServerId>

                    if target = userPrefs.LeaderUnderConsideration then
                        "(self)"
                    else
                        sprintf "%i" index
                )
                |> Seq.toList
                |> fun l -> "Will try next to send this index" :: l
                |> List.map (fun text ->
                    let node = document.createElement "div"
                    node.innerText <- text
                    Some node
                )

            Table.createRow document nextToSend ui.LeaderStateTable |> ignore

        Table.createHeaderRow document [ "Client ID" ; "Successful requests" ] ui.ClientsList

        // Clients
        for KeyValue (clientId, committed) in state.Clients do
            let clientNode =
                let node = document.createElement "div"
                node.innerText <- sprintf "%i" clientId
                node

            let messagesNode =
                let node = document.createElement "div"
                let text = committed |> Seq.map (sprintf "%i") |> String.concat ","

                node.innerText <- text
                node

            Table.createRow document [ Some clientNode ; Some messagesNode ] ui.ClientsList
            |> ignore

    let freezeState<'a> (cluster : Cluster<'a>) (network : Network<'a>) : ClusterState<'a> Async =
        let internalState =
            let states = Array.zeroCreate<ServerInternalState<'a>> cluster.ClusterSize

            [ 0 .. cluster.ClusterSize - 1 ]
            |> List.map (fun i -> cluster.GetCurrentInternalState (i * 1<ServerId>))
            |> List.fold
                (fun (i, acc) state ->
                    (i + 1),
                    async {
                        let! acc = acc
                        let! state = state
                        states.[i] <- state
                        return states
                    }
                )
                (0, async.Return states)
            |> snd

        let statuses =
            [|
                for i in 0 .. cluster.ClusterSize - 1 do
                    yield cluster.Status (i * 1<ServerId>)
            |]

        let undeliveredMessages =
            [|
                for i in 0 .. cluster.ClusterSize - 1 do
                    yield network.UndeliveredMessages (i * 1<ServerId>)
            |]

        let allMessages =
            [|
                for i in 0 .. cluster.ClusterSize - 1 do
                    yield network.AllInboundMessages (i * 1<ServerId>)
            |]

        async {
            let! internalState = internalState

            return
                {
                    ClusterSize = cluster.ClusterSize
                    InternalState = internalState
                    Statuses = statuses
                    UndeliveredMessages = undeliveredMessages
                    AllMessages = allMessages
                }
        }

    let getUserPrefs<'a>
        (parse : string -> Result<'a, string>)
        (handleRegisterClientResponse : RegisterClientResponse -> unit)
        (handleClientDataResponse : ClientResponse -> unit)
        (clusterSize : int)
        (ui : UiElements)
        : Result<UserPreferences<'a>, string>
        =
        let actionHistory =
            let arr = ResizeArray ()

            for i in StringSplitEnumerator.make '\n' ui.ActionHistoryList.value do
                if not (EfficientString.isEmpty i) then
                    NetworkAction.tryParse<'a>
                        parse
                        None
                        handleRegisterClientResponse
                        handleClientDataResponse
                        clusterSize
                        i
                    |> arr.Add

            Result.allOkOrError arr |> Result.map List.ofSeq

        match actionHistory with
        | Result.Ok actionHistory ->
            {
                LeaderUnderConsideration = ui.SelectedLeaderId.valueAsNumber |> int |> (fun i -> i * 1<ServerId>)
                ShowConsumedMessages = ui.ShowConsumedMessages.``checked``
                ActionHistory = actionHistory
            }
            |> Result.Ok
        | Result.Error e -> Result.Error (snd e |> String.concat "\n")
