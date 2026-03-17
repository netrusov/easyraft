package serializer

// Serializer interface is to provide serialize and deserialize methods for EasyRaft Node
type Serializer interface {
	// Serialize is used to serialize and data to a []byte
	Serialize(data any) ([]byte, error)

	// Deserialize is used to deserialize []byte to any
	Deserialize(data []byte) (any, error)
}
