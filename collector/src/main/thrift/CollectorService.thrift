#@namespace scala collector.service


service TCollectorService {
    oneway void add(1:string name,2:i32 version,3:string json)
}

