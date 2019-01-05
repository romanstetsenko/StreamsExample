open System.Threading

#r "./packages/streams.0.4.1/lib/net45/Streams.dll"

open System
open Nessos.Streams

type JsonA =
    { id : string }

type DbRequestA =
    { id : int }

type RawDataA =
    { id : int
      values : string array }

type ProcessedDataA =
    { id : int
      aggregatedValue : string }

type Guard = JsonA -> Result<DbRequestA, string>

type DbReader = DbRequestA -> Result<RawDataA, string>

type BussinesLogic =  RawDataA -> ProcessedDataA

type GenericWorkFlow = JsonA -> DbRequestA -> RawDataA


module Implementations =
    let private tryParse s =
        match Int32.TryParse s with
        | true, i -> Ok i
        | false, _ -> Error "Cannot parse int."
    
    let guard (arg : JsonA) : Result<DbRequestA, string> =
        arg.id
        |> tryParse
        |> Result.map (fun x -> { id = x })
    
    let private readEachValue x =
        printfn "Reading db value: %i " x
        Thread.Sleep(1000)
        printfn "Db value read: %i " x
        string x
    
    let private readEachValueAsync x =
        async { 
            printfn "Reading db value: %i " x
            Thread.Sleep(1000)
            printfn "Db value read: %i " x
            return (string x)
        }
    
    let dbReaderSequential (arg : DbRequestA) : Result<RawDataA, string> =
        try 
            if DateTime.Now.Millisecond % 3 = 0 then failwith "TIMEOUT."
            [| 1..arg.id |]
            |> Array.map readEachValue
            |> fun xs -> 
                { id = arg.id
                  values = xs }
            |> Ok
        with e -> Error e.Message
    
    let dbReaderParallel (arg : DbRequestA) : Result<RawDataA, string> =
        try 
            if DateTime.Now.Millisecond % 3 = 0 then failwith "TIMEOUT."
            [| 1..arg.id |]
            |> Array.map readEachValueAsync
            |> (Async.Parallel >> Async.RunSynchronously)
            |> fun xs -> 
                { id = arg.id
                  values = xs }
            |> Ok
        with e -> Error e.Message
    
    let bussinesLogic (arg : RawDataA) : ProcessedDataA =
        let processEachValue x =
            printfn "Processing item: %s " x
            Thread.Sleep(5000)
            printfn "Item processed: %s " x
            x
        
        let aggregatedValue =
            arg.values
            |> Array.map processEachValue
            |> String.concat ", "
        
        { id = arg.id
          aggregatedValue = aggregatedValue }
    
    let bussinesLogicMap x =
        printfn "Processing item: %s " x
        Thread.Sleep(5000)
        printfn "Item processed: %s " x
        x
    
    let bussinesLogicReduce id xs : Result<ProcessedDataA, string> =
        let aggregatedValue = xs |> String.concat ", "
        Ok { id = id
             aggregatedValue = aggregatedValue }

let runWf1Seq input =
    Stream.singleton input
    |> Stream.map Implementations.guard
    |> Stream.map (Result.bind Implementations.dbReaderSequential)
    |> Stream.map (Result.map Implementations.bussinesLogic)
    |> Stream.head

#time "on"

runWf1Seq ({ id = "3" } : JsonA)
runWf1Seq ({ id = "!@#$%^" } : JsonA)

let runWf1Par input =
    Stream.singleton input
    |> Stream.map Implementations.guard
    |> Stream.map (Result.bind Implementations.dbReaderParallel)
    |> Stream.map (Result.map Implementations.bussinesLogic)
    |> Stream.head

runWf1Par ({ id = "3" } : JsonA)
runWf1Par ({ id = "!@#$%^" } : JsonA)

let forkBL (input : RawDataA) =
    input.values
    |> Array.map 
           (fun x -> async { return (Implementations.bussinesLogicMap x) })
    |> (Async.Parallel
        >> Async.Catch
        >> Async.RunSynchronously)
    |> function 
    | Choice2Of2 e -> Error e.Message
    | Choice1Of2 xs -> Ok xs
    |> Result.bind (Implementations.bussinesLogicReduce input.id)

let runWf1Complicated input =
    Stream.singleton input
    |> Stream.map Implementations.guard
    |> Stream.map (Result.bind Implementations.dbReaderParallel)
    |> Stream.map (Result.map forkBL)
    |> Stream.head

runWf1Complicated ({ id = "3" } : JsonA)
runWf1Complicated ({ id = "!@#$%^" } : JsonA)
