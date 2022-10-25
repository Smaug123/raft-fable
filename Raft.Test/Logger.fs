namespace Raft.Test

[<RequireQualifiedAccess>]
module TestLogger =

    let make () : (string -> unit) * (unit -> string list) =
        let logs = ResizeArray ()
        let logLine (s : string) = lock logs (fun () -> logs.Add s)

        let freezeLogs () =
            lock logs (fun () -> logs |> Seq.toList)

        logLine, freezeLogs
