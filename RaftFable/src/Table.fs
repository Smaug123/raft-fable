namespace RaftFable

[<RequireQualifiedAccess>]
module Table =

    let createHeaderRow
        (document : Browser.Types.Document)
        (headings : string list)
        (table : Browser.Types.HTMLTableElement)
        : unit =
        let row = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement

        for heading in headings do
            let cell = document.createElement "th" :?> Browser.Types.HTMLTableCellElement

            cell.textContent <- heading
            row.appendChild cell |> ignore

        table.appendChild row |> ignore

    let createRow
        (document : Browser.Types.Document)
        (elements : seq<#Browser.Types.Node option>)
        (table : Browser.Types.HTMLTableElement)
        : unit =
        let row = document.createElement "tr" :?> Browser.Types.HTMLTableRowElement

        for col in elements do
            let entry = document.createElement "td" :?> Browser.Types.HTMLTableCellElement

            match col with
            | None -> ()
            | Some button -> entry.appendChild button |> ignore

            row.appendChild entry |> ignore

        table.appendChild row |> ignore
