#@namespace scala collector.service

struct TAnalyticRequest {
    1: string name,
    2: i32 version,
    3: string data
}
service TAnalyticService {
    oneway void add(1:TAnalyticRequest request)
}

