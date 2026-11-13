package service

import "errors"

var (
	ErrInvalidConfig               = errors.New("semantic service: invalid configuration")
	ErrInvalidLoadedState          = errors.New("semantic service: invalid loaded state")
	ErrModelServiceUnavailable     = errors.New("semantic service: model service cannot be reconstructed")
	ErrUnsupportedEncodingBehavior = errors.New("semantic service: unsupported encoding behavior version")
)
