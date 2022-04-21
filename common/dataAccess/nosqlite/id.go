package nosqlite

import "github.com/google/uuid"

func GenerateUUID() string {
	return uuid.Must(uuid.NewRandom()).String()
}
