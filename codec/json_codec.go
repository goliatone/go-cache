package codec

import "encoding/json"

// JSONCodec serializes values using encoding/json.
type JSONCodec[V any] struct{}

func NewJSONCodec[V any]() JSONCodec[V] {
	return JSONCodec[V]{}
}

func (JSONCodec[V]) Marshal(value V) ([]byte, error) {
	return json.Marshal(value)
}

func (JSONCodec[V]) Unmarshal(data []byte) (V, error) {
	var value V
	err := json.Unmarshal(data, &value)
	return value, err
}
