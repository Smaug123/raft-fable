#if NETSTANDARD2_0
namespace System.Runtime.CompilerServices

open System

[<Sealed ; AttributeUsage(AttributeTargets.Struct)>]
type IsByRefLikeAttribute () =
    inherit Attribute ()
#endif

namespace Raft

open System
open System.Collections
open System.Runtime.CompilerServices

type EfficientString =
#if NETSTANDARD2_0
    string
#else
    System.ReadOnlySpan<char>
#endif

[<RequireQualifiedAccess>]
module EfficientString =

    let inline isEmpty (s : EfficientString) : bool =
#if NETSTANDARD2_0
        s = ""
#else
        s.IsEmpty
#endif


    let inline ofString (s : string) : EfficientString =
#if NETSTANDARD2_0
        s
#else
        s.AsSpan ()
#endif

    let inline toString (s : EfficientString) : string =
#if NETSTANDARD2_0
        s
#else
        s.ToString ()
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

[<Struct>]
[<IsByRefLike>]
type StringSplitEnumerator =
    internal
        {
            Original : EfficientString
            mutable Remaining : EfficientString
            mutable InternalCurrent : EfficientString
            SplitOn : char
        }

    interface System.IDisposable with
        member this.Dispose () = ()

    member this.Current : EfficientString = this.InternalCurrent

    member this.MoveNext () =
        if this.Remaining.Length = 0 then
            false
        else
            this.InternalCurrent <- EfficientString.takeUntil this.SplitOn &this.Remaining
            true

    member this.GetEnumerator () = this

[<RequireQualifiedAccess>]
module StringSplitEnumerator =

    let make (splitChar : char) (s : string) : StringSplitEnumerator =
        {
            Original = EfficientString.ofString s
            Remaining = EfficientString.ofString s
            InternalCurrent = EfficientString.Empty
            SplitOn = splitChar
        }
