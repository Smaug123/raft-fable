namespace RaftFable

[<RequireQualifiedAccess>]
module Button =

    let create (document : Browser.Types.Document) (text : string) (onClick : Browser.Types.HTMLButtonElement -> 'a) =
        let node = document.createElement "button" :?> Browser.Types.HTMLButtonElement
        node.textContent <- text
        node.onclick <- fun _ -> onClick node
        node
