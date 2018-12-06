package tables

type Option interface {
	Run() error
}

type TableOverride struct {
}