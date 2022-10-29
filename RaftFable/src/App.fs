module App

open System
open Fable.Core.JS
open Raft
open Browser.Dom

let clusterSize = 5

let serverStatuses =
    document.querySelector ".server-statuses" :?> Browser.Types.HTMLTableElement

serverStatuses.border <- "1px"

do
    let row = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement

    let serverHeading =
        document.createElement "th" :?> Browser.Types.HTMLTableCellElement

    let statusHeading =
        document.createElement "th" :?> Browser.Types.HTMLTableCellElement

    serverHeading.textContent <- "Server"
    statusHeading.textContent <- "Status"
    row.appendChild serverHeading |> ignore
    row.appendChild statusHeading |> ignore
    serverStatuses.appendChild row |> ignore

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
    let headerRow = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement

    for i in 0 .. clusterSize - 1 do
        let heading = document.createElement "th" :?> Browser.Types.HTMLTableCellElement
        heading.innerText <- sprintf "Server %i" i
        headerRow.appendChild heading |> ignore

    messageQueueArea.appendChild headerRow |> ignore

resetButtonArea ()

let createButton (text : string) (onClick : Browser.Types.HTMLButtonElement -> unit) =
    let node = document.createElement "button" :?> Browser.Types.HTMLButtonElement
    node.textContent <- text
    node.onclick <- fun _ -> onClick node
    node

let printClusterState<'a> (cluster : Cluster<'a>) : unit =
    for i in 0 .. cluster.ClusterSize - 1 do
        serverStatusNodes.[i].textContent <- cluster.State (i * 1<ServerId>) |> string<ServerStatus>

let cluster, network = InMemoryCluster.make<string> clusterSize

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
                createButton
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
        let cellRow = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement

        for col in row do
            let entry = document.createElement "td" :?> Browser.Types.HTMLTableCellElement

            match col with
            | None -> ()
            | Some button -> entry.appendChild button |> ignore

            cellRow.appendChild entry |> ignore

        messageQueueArea.appendChild cellRow |> ignore

let perform action =
    performWithoutPrintingNetworkState action
    printNetworkState network

let getMessage (clusterSize : int) (s : string) : (int<ServerId> * int) option =
    match s.Split ',' with
    | [| serverId ; messageId |] ->
        let serverId = serverId.Trim ()
        let messageId = messageId.Trim ()

        match Int32.TryParse serverId with
        | true, serverId ->
            match Int32.TryParse messageId with
            | true, messageId ->
                if serverId >= clusterSize || serverId < 0 then
                    printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
                    None
                else
                    Some (serverId * 1<ServerId>, messageId)
            | false, _ ->
                printf "Non-integer input '%s' for message ID. " messageId
                None
        | false, _ ->
            printf "Non-integer input '%s' for server ID. " serverId
            None
    | _ ->
        printfn "Invalid input."
        None

let rec getHeartbeater (clusterSize : int) (serverId : string) =
    // TODO: restrict this to the leaders only
    match Int32.TryParse serverId with
    | true, serverId ->
        if serverId >= clusterSize || serverId < 0 then
            printf "Server ID must be between 0 and %i inclusive. " (clusterSize - 1)
            None
        else
            Some (serverId * 1<ServerId>)
    | false, _ ->
        printf "Unrecognised input. "
        None

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
