package entity

type Point struct {
	Timestamp int64       `json:"timestamp"`
	Value     interface{} `json:"value"`
}

type WriteSeries struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

type SeriesResult struct {
	Metric string            `json:"metric"`
	Labels map[string]string `json:"labels"`
	Points []Point           `json:"points"`
}

type QueryOptions struct {
	Metric string
	Labels map[string]string
	From   int64
	To     int64
}
