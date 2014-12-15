package common

type Quote struct {
	Value float64
}

func (q *Quote) GetQuote() float64 {
	return q.Value
}

func (q *Quote) SetQuote(v float64) {
	q.Value = v
}