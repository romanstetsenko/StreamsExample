open System.Threading
open System




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

type BussinesLogic =  RawDataA -> Result<ProcessedDataA, string>

type GenericWorkFlow = JsonA -> DbRequestA -> RawDataA -> ProcessedDataA


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
    
    let bussinesLogic (arg : RawDataA) : Result<ProcessedDataA, string> =
        let processEachValue x =
            printfn "Processing item: %s " x
            Thread.Sleep(3000)
            printfn "Item processed: %s " x
            x
        
        let aggregatedValue =
            arg.values
            |> Array.map processEachValue
            |> String.concat ", "
        
        Ok { id = arg.id; aggregatedValue = aggregatedValue }
    
    let bussinesLogicMap x =
        printfn "Processing item: %s " x
        Thread.Sleep(3000)
        printfn "Item processed: %s " x
        x
    
    let bussinesLogicReduce id xs : Result<ProcessedDataA, string> =
        let aggregatedValue = xs |> String.concat ", "
        Ok { id = id; aggregatedValue = aggregatedValue }


let wofkflowBuilder (guard: Guard) (dbReader: DbReader)  (bussinesLogic: BussinesLogic) = 
    //let result1 = guard input
    //let result2 = Result.bind dbReader result1
    //let result3 = Result.bind bussinesLogic result2
    //return result3
    guard >> Result.bind dbReader >> Result.bind bussinesLogic

#time "on"

//--------------------
let runWf1Seq input =
    let wf = 
        wofkflowBuilder 
            Implementations.guard 
            Implementations.dbReaderSequential 
            Implementations.bussinesLogic
    wf input

runWf1Seq ({ id = "3" } : JsonA)
runWf1Seq ({ id = "!@#$%^" } : JsonA)

//--------------------

let runWf1Par input =
    let wf = 
        wofkflowBuilder 
            Implementations.guard 
            Implementations.dbReaderParallel 
            Implementations.bussinesLogic
    wf input

runWf1Par ({ id = "3" } : JsonA)
runWf1Par ({ id = "!@#$%^" } : JsonA)

//--------------------

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
    let wf = 
        wofkflowBuilder 
            Implementations.guard 
            Implementations.dbReaderParallel 
            forkBL
    wf input

runWf1Complicated ({ id = "3" } : JsonA)
runWf1Complicated ({ id = "!@#$%^" } : JsonA)
