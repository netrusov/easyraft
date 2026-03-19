package serializer

import (
	"reflect"

	"github.com/hashicorp/go-msgpack/v2/codec"
)

type MsgpackSerializer struct {
}

var msgpackHandle = func() *codec.MsgpackHandle {
	var handle codec.MsgpackHandle
	handle.MapType = reflect.TypeFor[map[string]any]()
	handle.RawToString = true
	return &handle
}()

func NewMsgPackSerializer() Serializer {
	return &MsgpackSerializer{}
}

func (s *MsgpackSerializer) Serialize(data any) ([]byte, error) {
	var b []byte
	enc := codec.NewEncoderBytes(&b, msgpackHandle)
	if err := enc.Encode(data); err != nil {
		return nil, err
	}
	return b, nil
}

func (s *MsgpackSerializer) Deserialize(data []byte) (any, error) {
	var result any
	dec := codec.NewDecoderBytes(data, msgpackHandle)
	if err := dec.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
