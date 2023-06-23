package opentsdb

type AggregatorFuncT func(a, b Point) Point

func AggregatorFunc(v string) AggregatorFuncT {

	switch v {
	case "sum", "zimsum", "count":
		return func(a, b Point) Point { return a + b }
	case "avg":
		return func(a, b Point) Point { return (a + b) / 2 }
	case "max", "mimmax":
		return func(a, b Point) Point {
			if a > b {
				return a
			} else {
				return b
			}
		}
	case "min", "mimmin":
		return func(a, b Point) Point {
			if a > b {
				return a
			} else {
				return b
			}
		}
	case "dev", "first", "last": // nonense
		return func(a, b Point) Point { return (a + b) / 2 }
	default:
		panic(v)
	}

}

func (m DPmap) Join(n DPmap, agg string) DPmap {

	f := AggregatorFunc(agg)

	for k, v := range n {
		if v1, ok := m[k]; ok {
			m[k] = f(v1, v)
			continue
		}
		m[k] = v
	}
	return m
}
