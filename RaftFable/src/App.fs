namespace RaftFable

open Fable.Core.JS
open Raft
open Browser.Dom
open Fable.Core

module App =

    let clusterSize = 5

    let ui = Ui.initialise document clusterSize

    let rec fullyRerender<'a> (cluster : Cluster<'a>) (network : Network<'a>) : Promise<unit> =
        let prefs = Ui.getUserPrefs ui

        Ui.freezeState cluster network
        |> Async.StartAsPromise
        |> fun p ->
            p.``then`` (fun clusterState ->
                Ui.render
                    (perform cluster network)
                    document
                    ui
                    {
                        UserPreferences = prefs
                        ClusterState = clusterState
                    }
            )

    and perform (cluster : Cluster<'a>) (network : Network<'a>) (action : NetworkAction<'a>) : Promise<unit> =
        NetworkAction.perform cluster network action
        fullyRerender cluster network

    let cluster, network = InMemoryCluster.make<byte> clusterSize

    let leaderStateButton =
        document.querySelector ".leader-select-button" :?> Browser.Types.HTMLButtonElement

    leaderStateButton.onclick <- fun _ -> fullyRerender cluster network

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
        |> fun s -> (fullyRerender cluster network, s)
        ||> List.fold (fun (inPromise : Promise<unit>) action ->
            promise {
                let! _ = inPromise
                return! perform cluster network action
            }
        )

    let timeoutButton =
        document.querySelector ".timeout-button" :?> Browser.Types.HTMLButtonElement

    timeoutButton.onclick <-
        fun _event ->
            startupSequence.``then`` (fun () ->
                ui.TimeoutField.valueAsNumber
                |> int
                |> fun i -> i * 1<ServerId>
                |> InactivityTimeout
                |> perform cluster network
            )

    let heartbeatButton =
        document.querySelector ".heartbeat-button" :?> Browser.Types.HTMLButtonElement

    heartbeatButton.onclick <-
        fun _event ->
            startupSequence.``then`` (fun () ->
                ui.HeartbeatField.valueAsNumber
                |> int
                |> fun i -> i * 1<ServerId>
                |> Heartbeat
                |> perform cluster network
            )

    let clientDataSubmitButton =
        document.querySelector ".client-data-submit" :?> Browser.Types.HTMLButtonElement

    clientDataSubmitButton.onclick <-
        fun _event ->
            startupSequence.``then`` (fun () ->
                let server =
                    ui.ClientDataServerField.valueAsNumber |> int |> (fun i -> i * 1<ServerId>)

                let data = ui.ClientDataField.valueAsNumber |> byte

                NetworkAction.ClientRequest (server, data, printfn "%O")
                |> perform cluster network
            )
