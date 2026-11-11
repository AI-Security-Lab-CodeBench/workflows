package analyze

import (
	"bytes"
	"encoding/json"

	"mvdan.cc/sh/v3/syntax/typedjson"
)

func (d Document) MarshalJSON() ([]byte, error) {
	if d.Tree == nil {
		return nil, &Error{Kind: "ast", Detail: "document has no parsed tree"}
	}
	var tree bytes.Buffer
	if err := typedjson.Encode(&tree, d.Tree); err != nil {
		return nil, err
	}
	type documentFields Document
	return json.Marshal(struct {
		documentFields
		AST json.RawMessage `json:"ast"`
	}{documentFields(d), tree.Bytes()})
}
