package gpubsub

type messagetype int

const (
	Sub messagetype = iota
	Pub
)

type Message struct {
	Type  messagetype
	Topic string
	Data  []byte
}
