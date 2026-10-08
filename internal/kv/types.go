// Package kv 定义内存 KV 的请求、回复和存储。
package kv

type Version uint64
type Code string

const (
	OK               Code = "ok"
	NotFound         Code = "not_found"
	VersionConflict  Code = "version_conflict"
	InvalidArgument  Code = "invalid_argument"
	VersionExhausted Code = "version_exhausted"
)

type GetRequest struct {
	Key string `json:"key"`
}

type GetResponse struct {
	Code    Code    `json:"code"`
	Value   string  `json:"value"`
	Version Version `json:"version"`
}

type PutRequest struct {
	Key     string  `json:"key"`
	Value   string  `json:"value"`
	Version Version `json:"version"`
}

type PutResponse struct {
	Code    Code    `json:"code"`
	Version Version `json:"version"`
}
