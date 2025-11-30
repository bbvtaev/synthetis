
<img  width="1000"  height="300"  alt="Synthetis"  src="https://github.com/user-attachments/assets/08a90203-a58c-4c37-b1f0-ee817f58c8c5"  />

<h1  align="center">Synthetis v1.2.0-alpha</h1>

Stess test
```
BenchmarkWriteThroughput-10   453115   2288 ns/op  437053 ops/s   20 allocs/op
```

<h2 align="center">Docs</h2>
<h3>Write operation</h3>
It takes 3 params in func body. 

```go
func (db *synthetis.DB) Write(metric  string, labels  map[string]string, value  ...interface{}) error
```

```metric``` - stands for metric name 
```labels``` - stands for labels creation
```value``` - stands for points creation

```go
if  err  :=  sth.Write("each_millisecond_metric", map[string]string{"user": "pooser"}, rand.Intn(34)); err  !=  nil {
	slog.Info("Error occured", "err", err)
	return
}
```

<h3>Query operation</h3>

```go
func (db *synthetis.DB) Query(metric  string, labels  map[string]string, from  int64, to  int64) ([]entity.SeriesResult, error)
```

```metric``` - stands for metric name 
```labels``` - stands for label creation
```value``` - stands for points creation

```go
res, err  :=  sth.Query("each_millisecond_metric", map[string]string{"user": "pooser"}, 0, 0)

if  err  !=  nil {
	slog.Info("Error occured", "err", err)
	return
}
```