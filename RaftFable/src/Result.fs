namespace RaftFable

open System.Collections.Generic

[<RequireQualifiedAccess>]
module Result =

    let allOkOrError<'a, 'err>
        (results : Result<'a, 'err> seq)
        : Result<'a IReadOnlyList, 'a IReadOnlyList * 'err IReadOnlyList>
        =
        let okResults = ResizeArray ()
        let errResults = ResizeArray ()

        for r in results do
            match r with
            | Error e -> errResults.Add e
            | Ok o -> okResults.Add o

        let okResults = okResults :> IReadOnlyList<_>

        if errResults.Count = 0 then
            Ok okResults
        else
            Error (okResults, errResults :> IReadOnlyList<_>)

    let get<'a, 'err> (r : Result<'a, 'err>) : 'a =
        match r with
        | Ok o -> o
        | Error e -> failwithf "Tried to unwrap an error (%+A)" e
