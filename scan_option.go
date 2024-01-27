package squealx

type ScanOptions struct {
	StringifyRawBytes bool // stringifyRawBytes
}

type ScanOption func(opts *ScanOptions)

func loadScanOptions(options ...ScanOption) *ScanOptions {
	opts := new(ScanOptions)
	for _, option := range options {
		option(opts)
	}
	return opts
}

func StringifyRawBytes() ScanOption {
	return func(opts *ScanOptions) {
		opts.StringifyRawBytes = true
	}
}
