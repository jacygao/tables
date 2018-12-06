package tables

import (
	"errors"
)

var (
	ErrValidationFailed = errors.New("dynamodb table validation failed")

	ErrInvalidMigrationInput = errors.New("cannot migrate table input with unrecoverable errors")
)
