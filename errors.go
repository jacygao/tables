package tables

import (
	"errors"
)

var (
	ErrBackwardIncompatible = errors.New("table definition contains backward incompatible changes")

	ErrBackwardCompatible = errors.New("table definition contains backward compatible changes")

	ErrRequestWithMaxRetry = errors.New("request has reached the maximum number of retry attempts")

	ErrInvalidMigrationInput = errors.New("cannot migrate table input with unrecoverable errors")
)

func IsErrBackwardIncompatible(err error) bool {
	return err == ErrBackwardIncompatible
}
