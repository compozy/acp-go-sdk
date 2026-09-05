package acp

import "context"

// RequestResult is the final response to an asynchronously sent request.
type RequestResult[T any] struct {
	Value T
	Err   error
}

// SendRequestAsync returns after writing the request, before its response arrives.
// A successful return confirms transport submission, not peer acceptance. The
// buffered channel receives exactly one final result and then closes. Results
// retain the same notification-watermark ordering as SendRequest.
//
// The context owns the pending request and must be canceled when the caller no
// longer needs its result. Cancellation and peer death release the pending entry
// even if the caller never reads the result channel.
func SendRequestAsync[T any](c *Connection, ctx context.Context, method string, params any) (<-chan RequestResult[T], error) {
	pr, idKey, err := c.startRequest(ctx, method, params)
	if err != nil {
		return nil, err
	}
	result := make(chan RequestResult[T], 1)
	go func() {
		defer close(result)
		value, err := awaitRequestResponse[T](c, ctx, pr, idKey)
		result <- RequestResult[T]{Value: value, Err: err}
	}()
	return result, nil
}
