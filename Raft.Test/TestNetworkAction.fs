namespace Raft.Test

open Raft
open System.Collections.Generic
open NUnit.Framework
open FsCheck
open FsUnitTyped

[<TestFixture>]
module TestNetworkAction =

    [<Test>]
    let ``Generator hits all cases`` () =
        let populate (cases : HashSet<_>) (a : NetworkAction<'a>) : bool =
            cases.Add (a.GetType ()) |> ignore
            true

        let authoritativeCases = HashSet ()
        Check.QuickThrowOnFailure (populate authoritativeCases)

        let authoritativeCases =
            authoritativeCases |> Seq.map (fun ty -> ty.FullName) |> Set.ofSeq

        let ourCases = HashSet ()

        populate ourCases
        |> Prop.forAll (Arb.fromGen (NetworkAction.generate 5))
        |> Check.QuickThrowOnFailure

        let ourCases = ourCases |> Seq.map (fun ty -> ty.FullName) |> Set.ofSeq
        Set.difference authoritativeCases ourCases |> shouldBeEmpty

        // Sanity check that we are actually measuring what we think we're measuring
        ourCases
        |> Seq.map (fun name -> name.Split ('[') |> Array.head)
        |> Set.ofSeq
        |> shouldContain "Raft.NetworkAction`1+DropMessage"
