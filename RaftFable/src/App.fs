namespace RaftFable


open Fable.Core.JS
open Raft
open Browser.Dom

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

    let messageQueueArea =
        document.querySelector ".button-area" :?> Browser.Types.HTMLTableElement

    messageQueueArea.border <- "1px"

    let resetButtonArea () =
        messageQueueArea.innerText <- ""

        messageQueueArea
        |> Table.createHeaderRow document (List.init clusterSize (sprintf "Server %i"))

    resetButtonArea ()

    let printClusterState<'a> (cluster : Cluster<'a>) : unit =
        for i in 0 .. cluster.ClusterSize - 1 do
            serverStatusNodes.[i].textContent <- cluster.State (i * 1<ServerId>) |> string<ServerStatus>

    let cluster, network = InMemoryCluster.make<byte> clusterSize

    let performWithoutPrintingNetworkState action =
        NetworkAction.perform cluster network action
        printClusterState cluster

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
            Table.createRow document row messageQueueArea

    let perform (action : NetworkAction<_>) : unit =
        performWithoutPrintingNetworkState action
        printNetworkState network

    let startupText =
        document.querySelector ".startup-text" :?> Browser.Types.HTMLParagraphElement

    startupText.textContent <- "Starting up..."

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
        |> fun p -> p.``then`` (fun () -> startupText.textContent <- "Started! Press buttons.")

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
            )
