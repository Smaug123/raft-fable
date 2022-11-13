namespace Raft.Test

open FsCheck
open Raft

[<RequireQualifiedAccess>]
module NetworkAction =

    let generate<'a> (clusterSize : int) : Gen<NetworkAction<'a>> =
        gen {
            let! choice = Arb.generate<NetworkAction<'a>>
            let! server = Gen.choose (0, clusterSize - 1)
            let server = server * 1<ServerId>

            match choice with
            | NetworkAction.InactivityTimeout _ -> return NetworkAction.InactivityTimeout server
            | NetworkAction.NetworkMessage (_, message) -> return NetworkAction.NetworkMessage (server, abs message)
            | NetworkAction.DropMessage (_, message) -> return NetworkAction.DropMessage (server, abs message)
            | NetworkAction.Heartbeat _ -> return NetworkAction.Heartbeat server
            | NetworkAction.ClientRequest (_, ClientRequest.ClientRequest (client, sequence, data, func)) ->
                return
                    NetworkAction.ClientRequest (server, ClientRequest.ClientRequest (client, sequence, data, ignore))
            | NetworkAction.ClientRequest (_, ClientRequest.RegisterClient _) ->
                return NetworkAction.ClientRequest (server, ClientRequest.RegisterClient ignore)
        }

    let rec genNoClientRequests<'a> (clusterSize : int) : Gen<NetworkAction<'a>> =
        generate clusterSize
        |> Gen.filter (fun action ->
            match action with
            | NetworkAction.ClientRequest _ -> false
            | _ -> true
        )
