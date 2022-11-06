namespace RaftFable

open Fable.Core.JS
open Raft
open Browser.Dom
open Fable.Core

module App =

    let clusterSize = 5

    let ui = Ui.initialise document

    let rec fullyRerender<'a>
        (parse : string -> Result<'a, string>)
        (userPrefs : UserPreferences<'a> ref)
        (cluster : Cluster<'a>)
        (network : Network<'a>)
        : Promise<unit>
        =
        userPrefs.Value <- Ui.getUserPrefs<'a> parse cluster.ClusterSize ui

        Ui.freezeState cluster network
        |> Async.StartAsPromise
        |> fun p ->
            p.``then`` (fun clusterState ->
                Ui.render<'a>
                    (perform<'a> parse userPrefs cluster network)
                    document
                    ui
                    {
                        UserPreferences = userPrefs.Value
                        ClusterState = clusterState
                    }
            )

    and perform<'a>
        (parse : string -> Result<'a, string>)
        (userPrefs : UserPreferences<'a> ref)
        (cluster : Cluster<'a>)
        (network : Network<'a>)
        (action : NetworkAction<'a>)
        : Promise<unit>
        =
        NetworkAction.perform cluster network action

        userPrefs.Value <-
            { userPrefs.Value with
                ActionHistory = action :: userPrefs.Value.ActionHistory
            }

        fullyRerender parse userPrefs cluster network

    let parseByte (s : string) =
        match System.Byte.TryParse s with
        | false, _ -> Error (sprintf "Expected byte, got '%s'" s)
        | true, v -> Ok v

    let userPrefs : UserPreferences<byte> ref =
        ref (Ui.getUserPrefs parseByte clusterSize ui)

    let mutable cluster, network = InMemoryCluster.make<byte> clusterSize

    let leaderStateButton =
        document.querySelector ".leader-select-button" :?> Browser.Types.HTMLButtonElement

    leaderStateButton.onclick <- fun _ -> fullyRerender parseByte userPrefs cluster network

    let startupActions : NetworkAction<byte> list =
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

    ui.ActionHistory.textContent <- startupActions |> Seq.map NetworkAction.toString |> String.concat "\n"

    let reloadActions () =
        let newCluster, newNetwork = InMemoryCluster.make<byte> clusterSize
        cluster <- newCluster
        network <- newNetwork

        userPrefs.Value <- Ui.getUserPrefs parseByte clusterSize ui

        startupActions
        |> fun s -> (fullyRerender parseByte userPrefs cluster network, s)
        ||> List.fold (fun (inPromise : Promise<unit>) action ->
            promise {
                let! _ = inPromise
                return! perform parseByte userPrefs cluster network action
            }
        )

    let reloadActionsButton =
        document.querySelector ".reload-actions" :?> Browser.Types.HTMLButtonElement

    reloadActionsButton.onclick <- fun _evt -> reloadActions ()

    reloadActions () |> ignore

    let timeoutButton =
        document.querySelector ".timeout-button" :?> Browser.Types.HTMLButtonElement

    timeoutButton.onclick <-
        fun _event ->
            ui.TimeoutField.valueAsNumber
            |> int
            |> fun i -> i * 1<ServerId>
            |> InactivityTimeout
            |> perform parseByte userPrefs cluster network

    let heartbeatButton =
        document.querySelector ".heartbeat-button" :?> Browser.Types.HTMLButtonElement

    heartbeatButton.onclick <-
        fun _event ->
            ui.HeartbeatField.valueAsNumber
            |> int
            |> fun i -> i * 1<ServerId>
            |> Heartbeat
            |> perform parseByte userPrefs cluster network

    let clientDataSubmitButton =
        document.querySelector ".client-data-submit" :?> Browser.Types.HTMLButtonElement

    clientDataSubmitButton.onclick <-
        fun _event ->
            let server =
                ui.ClientDataServerField.valueAsNumber |> int |> (fun i -> i * 1<ServerId>)

            let data = ui.ClientDataField.valueAsNumber |> byte

            NetworkAction.ClientRequest (server, data)
            |> perform parseByte userPrefs cluster network

    ui.ShowConsumedMessages.onchange <- fun _event -> fullyRerender parseByte userPrefs cluster network
