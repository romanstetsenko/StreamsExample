open System.Threading
open System

type ValuableData =
    | Json of string
    | DbRequest of int
    | DataSet of int * string array
    | Processed of int * string
    | Reduce of int * string array
    | Map of string

type Guard = ValuableData -> Result<ValuableData, string>

type DbReader = ValuableData -> Result<ValuableData, string>

type BussinesLogic = ValuableData -> Result<ValuableData, string>

type GenericWorkFlow = ValuableData -> ValuableData -> ValuableData -> ValuableData

module Implementations =

    let randomlyWait (x : int) =
        let delta = (float x) * 0.05 |> int
        Random().Next(x - delta, x + delta) |> Thread.Sleep
    
    let private tryParse s =
        match Int32.TryParse s with
        | true, i -> Ok i
        | false, _ -> Error "Cannot parse int."
    
    let guard (Json someId) : Result<ValuableData, string> =
        someId
        |> tryParse
        |> Result.map DbRequest
    
    let private readEachValue x =
        randomlyWait 200
        printfn "Reading db value: %i " x
        randomlyWait 800
        printfn "Db value read: %i " x
        string x
    
    let private readEachValueAsync x =
        async { 
            randomlyWait 200
            printfn "Reading db value: %i " x
            randomlyWait 800
            printfn "Db value read: %i " x
            return (string x)
        }
    
    let dbReaderSequential (DbRequest id) : Result<ValuableData, string> =
        try 
            if DateTime.Now.Millisecond % 3 = 0 then failwith "TIMEOUT."
            [| 1..id |]
            |> Array.map readEachValue
            |> fun values -> DataSet(id, values)
            |> Ok
        with e -> Error e.Message
    
    let dbReaderParallel (DbRequest id) : Result<ValuableData, string> =
        try 
            if DateTime.Now.Millisecond % 3 = 0 then failwith "TIMEOUT."
            [| 1..id |]
            |> Array.map readEachValueAsync
            |> (Async.Parallel >> Async.RunSynchronously)
            |> fun xs -> DataSet(id, xs)
            |> Ok
        with e -> Error e.Message
    
    let bussinesLogic (DataSet(id, data)) : Result<ValuableData, string> =
        let processEachValue x =
            randomlyWait 200
            printfn "Processing item: %s " x
            randomlyWait 2800
            printfn "Item processed: %s " x
            x
        
        let aggregatedValue =
            data
            |> Array.map processEachValue
            |> String.concat ", "
        
        Ok(Processed(id, aggregatedValue))
    
    let bussinesLogicMap (Map x) =
        randomlyWait 200
        printfn "Processing item: %s " x
        randomlyWait 2800
        printfn "Item processed: %s " x
        x
    
    let bussinesLogicReduce (Reduce(id, xs)) : Result<ValuableData, string> =
        let aggregatedValue = xs |> String.concat ", "
        Ok(Processed(id, aggregatedValue))

module Posibilities =
    type Handler = Handler of (ValuableData -> Result<ValuableData, string>)
    
    let someHandlers =
        [| Handler Implementations.bussinesLogic
           Handler Implementations.dbReaderSequential
           Handler Implementations.bussinesLogic |]
    
    let _loggedSomeHandlers =
        someHandlers
        |> Array.map (fun (Handler handler) -> 
               let wrapper valuableData =
                   printf "LOGING INPUT: %A" valuableData
                   let result = handler valuableData
                   printf "LOGING OUTPUT: %A" result
                   result //return 
               Handler wrapper)

let wofkflowBuilder (guard : Guard) (dbReader : DbReader) 
    (bussinesLogic : BussinesLogic) =
    //let guardResult = guard input
    //if guardResult is Ok  
    //then 
    //    let dbReaderResult = dbReader guardResult.Value
    //    if dbReaderResult is Ok then 
    //        let bussinesLogicResult = bussinesLogic dbReaderResult.Value
    //        if bussinesLogicResult is Ok 
    //            then return bussinesLogicResult
    //        else throw
    //    else throw
    //else throw
    guard
    >> Result.bind dbReader
    >> Result.bind bussinesLogic

#time "on"

//--------------------
let runWf1Seq input =
    printfn "//// Sequential ////"
    let wf =
        wofkflowBuilder 
            Implementations.guard 
            Implementations.dbReaderSequential 
            Implementations.bussinesLogic
    wf input

runWf1Seq (Json "3")
runWf1Seq (Json "!@#$%^")

//--------------------
let runWf1Par input =
    printfn "//// Sequential + dbReaderParallel ////"
    let wf =
        wofkflowBuilder 
            Implementations.guard 
            Implementations.dbReaderParallel 
            Implementations.bussinesLogic
    wf input

runWf1Par (Json "3")
runWf1Par (Json "!@#$%^")

//--------------------
let forkBL (DataSet(id, values)) =
    values
    |> Array.Parallel.map (fun value -> Implementations.bussinesLogicMap (Map value))
    |> (fun values -> Implementations.bussinesLogicReduce (Reduce(id, values)))

let runWf1Complicated input =
    printfn "//// Parallel ////"
    let wf =
        wofkflowBuilder 
            Implementations.guard 
            Implementations.dbReaderParallel 
            forkBL
    wf input

runWf1Complicated (Json "5")
runWf1Complicated (Json "!@#$%^")
