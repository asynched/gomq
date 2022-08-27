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
