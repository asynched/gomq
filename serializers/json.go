package serializers

import (
	"encoding/json"
)

func FromJson[T any](data []byte) (T, error) {
	var serialized T

	err := json.Unmarshal(data, &serialized)

	if err != nil {
		return serialized, err
	}

	return serialized, nil
}

func ToJson[T any](data T) ([]byte, error) {
	return json.Marshal(data)
}
