namespace Raft.Test

[<RequireQualifiedAccess>]
module Result =

    let get<'a, 'b> (r : Result<'a, 'b>) : 'a =
        match r with
        | Ok a -> a
        | Error e -> failwithf "%+A" e
