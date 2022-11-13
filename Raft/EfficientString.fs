namespace Raft

open System

type internal EfficientString =
#if NETSTANDARD2_0
    string
#else
    System.ReadOnlySpan<char>
#endif

[<RequireQualifiedAccess>]
module internal EfficientString =

    let inline ofString (s : string) : EfficientString =
#if NETSTANDARD2_0
        s
#else
        s.AsSpan ()
#endif

    let inline trimStart (s : EfficientString) : EfficientString = s.TrimStart ()

    let inline slice (start : int) (length : int) (s : EfficientString) : EfficientString =
#if NETSTANDARD2_0
        s.[start .. start + length - 1]
#else
        s.Slice (start, length)
#endif

    /// Mutates the input to drop up to the first instance of the input char,
    /// and returns what was dropped.
    /// If the char is not present, deletes the input.
    let takeUntil<'a> (c : char) (s : EfficientString byref) : EfficientString =
        let first = s.IndexOf c

        if first < 0 then
            let toRet = s
            s <- EfficientString.Empty
            toRet
        else
            let toRet = slice 0 first s
            s <- slice (first + 1) (s.Length - first - 1) s
            toRet
