package serializer

import (
	"github.com/vmihailenco/msgpack/v5"
)

func NewMsgPackSerializer() Serializer {
	return &ConverterService{}
}

type ConverterService struct {
}

func (s *ConverterService) Serialize(data any) ([]byte, error) {
	return msgpack.Marshal(data)
}

func (s *ConverterService) Deserialize(data []byte) (any, error) {
	var result any
	err := msgpack.Unmarshal(data, &result)
	return result, err
}
