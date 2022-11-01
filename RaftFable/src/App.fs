namespace RaftFable

open Fable.Core.JS
open Raft
open Browser.Dom
open Fable.Core

module App =

    let clusterSize = 5

    let serverStatuses =
        document.querySelector ".server-statuses" :?> Browser.Types.HTMLTableElement

    serverStatuses.border <- "1px"
    Table.createHeaderRow document [ "Server" ; "Status" ] serverStatuses

    let serverStatusNodes =
        fun i ->
            let node = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement
            let child = document.createElement "td" :?> Browser.Types.HTMLTableCellElement
            let statusCell = document.createElement "td" :?> Browser.Types.HTMLTableCellElement
            child.textContent <- sprintf "%i" i
            node.appendChild child |> ignore
            node.appendChild statusCell |> ignore
            serverStatuses.appendChild node |> ignore
            statusCell
        |> List.init clusterSize

    let logArea = document.querySelector ".log-area" :?> Browser.Types.HTMLTableElement

    logArea.border <- "1px"

    let renderLogArea (cluster : Cluster<'a>) =
        logArea.innerText <- ""

        let rows =
            List.init
                cluster.ClusterSize
                (fun i ->
                    let cell = document.createElement "th" :?> Browser.Types.HTMLTableCellElement
                    cell.textContent <- sprintf "Server %i" i
                    Table.createRow document [ Some cell ] logArea
                )

        rows
        |> List.iteri (fun i row ->
            let i = i * 1<ServerId>

            cluster.GetCurrentInternalState i
            |> Async.StartAsPromise
            |> fun p ->
                p.``then`` (fun state ->
                    for logEntry in state.Log do
                        let cell = document.createElement "td" :?> Browser.Types.HTMLTableCellElement

                        match logEntry with
                        | None -> cell.textContent <- "<none>"
                        | Some (value, term) -> cell.textContent <- sprintf "%i: %O" term value

                        row.appendChild cell |> ignore
                )
            |> ignore
        )

    let messageQueueArea =
        document.querySelector ".button-area" :?> Browser.Types.HTMLTableElement

    messageQueueArea.border <- "1px"

    let resetButtonArea () =
        messageQueueArea.innerText <- ""

        messageQueueArea
        |> Table.createHeaderRow document (List.init clusterSize (sprintf "Server %i"))

    resetButtonArea ()

    let setLeaderState (cluster : Cluster<'a>) (id : int<ServerId>) =
        let leaderIdBox =
            document.querySelector ".leader-state" :?> Browser.Types.HTMLBlockElement

        leaderIdBox.innerText <- sprintf "%i" id

        let leaderIdTable =
            document.querySelector ".leader-state-table" :?> Browser.Types.HTMLTableElement

        leaderIdTable.innerText <- ""

        leaderIdTable
        |> Table.createHeaderRow document ("" :: List.init clusterSize (sprintf "Server %i"))

        let leaderStatePromise = cluster.GetCurrentInternalState id |> Async.StartAsPromise

        leaderStatePromise.``then`` (fun state ->
            match state.LeaderState with
            | None -> leaderIdBox.innerText <- sprintf "%i: not a leader" id
            | Some state ->

            let knownStoredIndices =
                state.MatchIndex
                |> Seq.mapi (fun target index ->
                    let target = target * 1<ServerId>
                    if target = id then "(self)" else sprintf "%i" index
                )
                |> Seq.toList
                |> fun l -> "Log index known to be stored on each node" :: l
                |> List.map (fun text ->
                    let node = document.createElement "div"
                    node.innerText <- text
                    Some node
                )

            Table.createRow document knownStoredIndices leaderIdTable |> ignore

            let nextToSend =
                state.ToSend
                |> Seq.mapi (fun target index ->
                    let target = target * 1<ServerId>
                    if target = id then "(self)" else sprintf "%i" index
                )
                |> Seq.toList
                |> fun l -> "Will try next to send this index" :: l
                |> List.map (fun text ->
                    let node = document.createElement "div"
                    node.innerText <- text
                    Some node
                )

            Table.createRow document nextToSend leaderIdTable |> ignore
        )

    let printClusterState<'a> (cluster : Cluster<'a>) : unit =
        for i in 0 .. cluster.ClusterSize - 1 do
            let status = cluster.Status (i * 1<ServerId>)
            serverStatusNodes.[i].textContent <- status |> string<ServerStatus>

    let cluster, network = InMemoryCluster.make<byte> clusterSize

    let leaderStateButton =
        document.querySelector ".leader-select-button" :?> Browser.Types.HTMLButtonElement

    let selectedLeaderId =
        document.querySelector ".leader-select" :?> Browser.Types.HTMLInputElement

    selectedLeaderId.min <- "0"
    selectedLeaderId.max <- sprintf "%i" (clusterSize - 1)
    selectedLeaderId.defaultValue <- "0"

    leaderStateButton.onclick <-
        fun _ ->
            let id = selectedLeaderId.valueAsNumber |> int |> (fun i -> i * 1<ServerId>)
            setLeaderState cluster id

    let performWithoutPrintingNetworkState action =
        NetworkAction.perform cluster network action
        printClusterState cluster
        renderLogArea cluster

    let rec printNetworkState<'a> (network : Network<'a>) : unit =
        resetButtonArea ()

        let allButtons =
            [ 0 .. network.ClusterSize - 1 ]
            |> List.map (fun i ->
                let i = i * 1<ServerId>

                network.UndeliveredMessages i
                |> List.map (fun (messageId, message) ->
                    Button.create
                        document
                        (sprintf "Server %i, message %i: %O" i messageId message)
                        (fun button ->
                            button.remove ()
                            NetworkMessage (i, messageId) |> performWithoutPrintingNetworkState
                            printNetworkState network
                        )
                )
            )

        let maxQueueLength = allButtons |> Seq.map List.length |> Seq.max

        let allButtons' =
            allButtons
            |> List.map (fun l -> List.append (List.map Some l) (List.replicate (maxQueueLength - List.length l) None))
            |> List.transpose

        for row in allButtons' do
            Table.createRow document row messageQueueArea |> ignore

    let perform (action : NetworkAction<_>) : unit =
        performWithoutPrintingNetworkState action
        printNetworkState network

    let startupText =
        document.querySelector ".startup-text" :?> Browser.Types.HTMLParagraphElement

    let startupSequence =
        [
            NetworkAction.InactivityTimeout 0<ServerId>
            NetworkAction.InactivityTimeout 1<ServerId>
            // Two servers vote for server 1...
            NetworkAction.NetworkMessage (2<ServerId>, 1)
            NetworkAction.NetworkMessage (3<ServerId>, 1)
            // One server votes for server 0...
            NetworkAction.NetworkMessage (4<ServerId>, 0)
            // and the other votes are processed and discarded
            NetworkAction.NetworkMessage (0<ServerId>, 0)
            NetworkAction.NetworkMessage (1<ServerId>, 0)
            NetworkAction.NetworkMessage (2<ServerId>, 0)
            NetworkAction.NetworkMessage (3<ServerId>, 0)
            NetworkAction.NetworkMessage (4<ServerId>, 1)
            // Server 0 process incoming votes
            NetworkAction.NetworkMessage (0<ServerId>, 1)
            // Server 1 processes incoming votes, and achieves majority, electing itself leader!
            NetworkAction.NetworkMessage (1<ServerId>, 1)
            NetworkAction.NetworkMessage (1<ServerId>, 2)
            // Get the followers' heartbeat processing out of the way
            NetworkAction.NetworkMessage (2<ServerId>, 2)
            NetworkAction.NetworkMessage (3<ServerId>, 2)
            NetworkAction.NetworkMessage (4<ServerId>, 2)
            NetworkAction.NetworkMessage (1<ServerId>, 3)
            NetworkAction.NetworkMessage (1<ServerId>, 4)
            NetworkAction.NetworkMessage (1<ServerId>, 5)
            // Server 0 processes the leader's heartbeat and drops out of the election.
            NetworkAction.NetworkMessage (0<ServerId>, 2)
            NetworkAction.NetworkMessage (1<ServerId>, 6)
        ]
        |> fun s -> (Constructors.Promise.resolve (printClusterState cluster), s)
        ||> List.fold (fun (inPromise : Promise<unit>) action -> inPromise.``then`` (fun () -> perform action))
        |> fun p -> p.``then`` (fun () -> startupText.textContent <- "")

    let timeoutButton =
        document.querySelector ".timeout-button" :?> Browser.Types.HTMLButtonElement

    let timeoutField =
        document.querySelector ".timeout-text" :?> Browser.Types.HTMLInputElement

    timeoutField.max <- string<int> (clusterSize - 1)
    timeoutField.min <- "0"
    timeoutField.defaultValue <- "0"

    timeoutButton.onclick <-
        fun _event ->
            startupSequence.``then`` (fun () ->
                timeoutField.valueAsNumber
                |> int
                |> fun i -> i * 1<ServerId>
                |> InactivityTimeout
                |> perform

                printClusterState cluster
                renderLogArea cluster
                printNetworkState network
            )

    let heartbeatButton =
        document.querySelector ".heartbeat-button" :?> Browser.Types.HTMLButtonElement

    let heartbeatField =
        document.querySelector ".heartbeat-text" :?> Browser.Types.HTMLInputElement

    heartbeatField.max <- string<int> (clusterSize - 1)
    heartbeatField.min <- "0"
    heartbeatField.defaultValue <- "0"

    heartbeatButton.onclick <-
        fun _event ->
            startupSequence.``then`` (fun () ->
                heartbeatField.valueAsNumber
                |> int
                |> fun i -> i * 1<ServerId>
                |> Heartbeat
                |> perform

                printClusterState cluster
                renderLogArea cluster
                printNetworkState network
            )

    let clientDataField =
        document.querySelector ".client-data" :?> Browser.Types.HTMLInputElement

    clientDataField.max <- "255"
    clientDataField.min <- "0"
    clientDataField.defaultValue <- "0"

    let clientDataServerField =
        document.querySelector ".client-server-selection" :?> Browser.Types.HTMLInputElement

    clientDataServerField.max <- string<int> (clusterSize - 1)
    clientDataServerField.min <- "0"
    clientDataServerField.defaultValue <- "0"

    let clientDataSubmitButton =
        document.querySelector ".client-data-submit" :?> Browser.Types.HTMLButtonElement

    clientDataSubmitButton.onclick <-
        fun _event ->
            startupSequence.``then`` (fun () ->
                let server =
                    clientDataServerField.valueAsNumber |> int |> (fun i -> i * 1<ServerId>)

                let data = clientDataField.valueAsNumber |> byte
                NetworkAction.ClientRequest (server, data, printfn "%O") |> perform

                printClusterState cluster
                renderLogArea cluster
                printNetworkState network
            )
