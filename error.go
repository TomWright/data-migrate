package data_migrate

const (
	// ErrSkipRow should be returned from a ProcessRowFunc when the given row should not be copied
	// to the destination table.
	ErrSkipRow = errType("row should be skipped")
)

type errType string

func (e errType) Error() string {
	return string(e)
}
