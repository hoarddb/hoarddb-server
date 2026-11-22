package wire

// WithGetManyHeld lets an external test (package wire_test) watch the bytes of answers a
// RING_GET_MANY frame holds each time one of its reads lands. It exists in a test file so
// production code carries no such hook.
func WithGetManyHeld(held func(bytes int64)) RingOption {
	return func(s *ringServer) { s.manyHeld = held }
}
