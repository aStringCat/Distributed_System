package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"system/internal/kv"
)

var errInvalidJSON = errors.New("invalid JSON")

const maxRequestBody = 1 << 20

const requestTimeout = 3 * time.Second

func NewHandler(s *Replicated) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /kv", withRequest(s.handleGet))
	mux.HandleFunc("PUT /kv", withRequest(s.handlePut))
	return mux
}

func withRequest(next func(http.ResponseWriter, *http.Request, RequestID)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id, err := readRequestID(r)
		if err != nil {
			writeRequestError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), requestTimeout)
		defer cancel()
		next(w, r.WithContext(ctx), id)
	}
}

func readRequestID(r *http.Request) (RequestID, error) {
	seq, err := strconv.ParseUint(r.Header.Get("X-Request-Seq"), 10, 64)
	id := RequestID{ClientID: r.Header.Get("X-Client-ID"), Seq: seq}
	if err != nil || !id.valid() {
		return RequestID{}, ErrInvalidRequestID
	}
	return id, nil
}

func (s *Replicated) handleGet(w http.ResponseWriter, r *http.Request, id RequestID) {
	req := kv.GetRequest{Key: r.URL.Query().Get("key")}
	res, err := s.Get(r.Context(), id, req)
	if err != nil {
		writeRequestError(w, err)
		return
	}
	writeResponse(w, res.Code, res)
}

func (s *Replicated) handlePut(w http.ResponseWriter, r *http.Request, id RequestID) {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	data, err := io.ReadAll(r.Body)
	if err != nil {
		writeRequestError(w, err)
		return
	}
	var req kv.PutRequest
	if err := json.Unmarshal(data, &req); err != nil {
		writeRequestError(w, errInvalidJSON)
		return
	}
	res, err := s.Put(r.Context(), id, req)
	if err != nil {
		writeRequestError(w, err)
		return
	}
	writeResponse(w, res.Code, res)
}

func writeResponse(w http.ResponseWriter, code kv.Code, res any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(responseStatus(code))
	_ = json.NewEncoder(w).Encode(res)
}

func responseStatus(code kv.Code) int {
	switch code {
	case kv.OK:
		return http.StatusOK
	case kv.NotFound:
		return http.StatusNotFound
	case kv.InvalidArgument:
		return http.StatusBadRequest
	case kv.VersionConflict, kv.VersionExhausted:
		return http.StatusConflict
	default:
		return http.StatusInternalServerError
	}
}

func writeRequestError(w http.ResponseWriter, err error) {
	status := http.StatusInternalServerError
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		status = http.StatusRequestEntityTooLarge
	case errors.Is(err, errInvalidJSON), errors.Is(err, ErrInvalidRequestID):
		status = http.StatusBadRequest
	case errors.Is(err, ErrStopped), errors.Is(err, ErrNotLeader):
		status = http.StatusServiceUnavailable
	case errors.Is(err, context.DeadlineExceeded):
		status = http.StatusGatewayTimeout
	case errors.Is(err, context.Canceled):
		status = http.StatusRequestTimeout
	}
	http.Error(w, http.StatusText(status), status)
}
