package codec

import (
	"encoding/json"
	"errors"
	"testing"
)

type jsonCodecFixture struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

func TestJSONCodecRoundTrip(t *testing.T) {
	c := NewJSONCodec[jsonCodecFixture]()
	in := jsonCodecFixture{Name: "ok", Count: 7}

	raw, err := c.Marshal(in)
	if err != nil {
		t.Fatalf("unexpected marshal error: %v", err)
	}

	out, err := c.Unmarshal(raw)
	if err != nil {
		t.Fatalf("unexpected unmarshal error: %v", err)
	}

	if out != in {
		t.Fatalf("expected %+v, got %+v", in, out)
	}
}

func TestJSONCodecUnmarshalFailure(t *testing.T) {
	c := NewJSONCodec[jsonCodecFixture]()
	_, err := c.Unmarshal([]byte(`{"name":"broken","count":`))
	if err == nil {
		t.Fatal("expected unmarshal error")
	}
	var syntaxErr *json.SyntaxError
	if !errors.As(err, &syntaxErr) {
		t.Fatalf("expected syntax-like error, got %T", err)
	}
}
